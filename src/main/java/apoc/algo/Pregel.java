package apoc.algo;

import apoc.Pools;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.impl.api.RelationshipVisitor;
import org.neo4j.kernel.impl.api.store.RelationshipIterator;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * @author mh
 * @since 21.07.16
 */
public class Pregel {
    /*
     # of threads ?? depends if the operation is io-bound or CPU bound
     consider using LMAX disruptor?
     Pregel:
     BSP algorithm, called for each relationship, (rel-id, start, end-node, statement, state) concurrently
     Superstep called after all concurrent steps have been processed with all states, returns true if algorithm is to continue
     Also returns "combined" state?

     works on top of a relationship-datastructure start-node->end-node
     ordered ? node source, PrimitiveLongIterator from cypher query, index lookup, id-range, label scan etc.
     concurrently read in batches from node-source
     configure rel-type + direction for consideration
     (optionally also property predicates, or potentially)
     if the percentage * avg-degree is much lower than the number of rels, work via per node relationship-chains
     i.e. turn node source into a relationship source

     or use relationship-source directly, i.e. filter relationships by start/end-node-ids or rel-type or property
     ((roaring)-bitset (bitset-array indexed by upper-word) ?
     only do the filtering once, then use an alternative data structure to store the information

     node-source -> rel-source -> ds (hilbert?)

     optimized datastructure for relationship-information
     consider grouping by type, direction, start-node
     algorithm should probably take an interface, e.g. a Rel-Record, which can be a copy or a flightweight
     or a wrapper around a byte or other buffer

     investigate if each program keeps its state and one program is created per batch and returned with the batch (runProgram3)
     will have low memory efficiency but less object creation

     todo better: eager merge operation(s) to consume STATE and return it to the pool

     todo redistribute rel-batches potentially group them, optimize locality per algorithm

      pass object-pool to state(pool) and states merge(states, state, pool) and optionally to next(states, pool)

      fork-join-pool with work stealing but minimal object creation

      memory vs. object allocation vs. disk/store-access

      node-source -> expanded-rels (stream or stored batches) -> run programs -> accumulate states (merging, incrementally)

    */

    GraphDatabaseAPI api;
    private ThreadToStatementContextBridge ctx;
    private int batchSize = 10_000;
    private ExecutorService pool = Pools.DEFAULT;

    public Pregel(GraphDatabaseAPI api) {
        this.api = api;
        ctx = api.getDependencyResolver().resolveDependency(ThreadToStatementContextBridge.class);
    }

    public Pregel withBatchSize(int batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    public Pregel withPool(ExecutorService pool) {
        this.pool = pool;
        return this;
    }

    public <STATE, RESULT> RESULT runProgram(PrimitiveLongIterator nodes, NodeExpander expander,
                                             PregelProgram<STATE, RESULT> program) throws Exception {
        RESULT result;
        List<long[]> batches = batchLongIterator(nodes);
        List<Object> states = new ArrayList<>(1024);
        do {
            /*
            List<STATE> states = batchLongIterator(nodes).stream().map(batch -> runBatch(expander, program, batch))
                    .map(this::get)
                    .collect(Collectors.toList());
            */
            states.clear();
            for (long[] batch : batches) {
                states.add(runBatch(expander, program, batch));
            }
            resolveFutures(states);
            result = program.next((List<STATE>) states);
        } while (result == null);
        return result;
    }

    public <STATE, RESULT> RESULT runProgram2(PrimitiveLongIterator nodes, NodeExpander expander,
                                              PregelProgram<STATE, RESULT> program) throws Exception {
        RESULT result;

        List<long[]> relBatches = collectRelationshipBatches(nodes, expander);

        List<Object> states = new ArrayList<>(1024);
        do {
            states.clear();
            for (long[] relBatch : relBatches) {
                states.add(runRelBatch(program, relBatch));
            }
            resolveFutures(states);
            result = program.next((List<STATE>) states);
        } while (result == null);
        return result;
    }

    /*
    keeps state in program
    */
    public <STATE, RESULT> RESULT runProgram3(PrimitiveLongIterator nodes, NodeExpander expander,
                                              PregelProgram<STATE, RESULT> program) throws Exception {
        RESULT result;

        List<long[]> relBatches = collectRelationshipBatches(nodes, expander);

        IdentityHashMap<PregelProgram<STATE,RESULT>,long[]> programs = new IdentityHashMap<>(relBatches.size());
        for (long[] relBatch : relBatches) {
            programs.put(program.newInstance(),relBatch);
        }

        List<Object> states = new ArrayList<>(1024);
        do {
            states.clear();
            for (Map.Entry<PregelProgram<STATE, RESULT>, long[]> entry : programs.entrySet()) {
                states.add(runRelBatch(entry.getKey(), entry.getValue()));
            }
            resolveFutures(states);
            result = program.next((List<STATE>) states);
        } while (result == null);
        return result;
    }

    private void resolveFutures(List<Object> states) throws InterruptedException, ExecutionException {
        int numStates = states.size();
        // reuse list to save memory, hack
        for (int i = 0; i < numStates; i++) {
            states.set(i, ((Future) states.get(i)).get());
        }
    }

    private List<long[]> collectRelationshipBatches(PrimitiveLongIterator nodes, NodeExpander expander) throws InterruptedException, ExecutionException {
        List<Future<long[]>> states = new ArrayList<>(1024);
        while (nodes.hasNext()) {
            long[] batch = new long[batchSize];
            grabBatch(nodes, batch);

            Future<long[]> future = pool.submit(() -> {
                try (Transaction tx = api.beginTx()) {
                    Statement statement = statement();
                    CollectingRelationshipVisitor visitor = new CollectingRelationshipVisitor();
                    for (long node : batch) {
                        if (node == -1) break;
                        expander.expand(node, statement, visitor);
                    }
                    tx.success();
                    return visitor.getRelBatch();
                }
            });
            states.add(future);
        }
        List<long[]> relBatches = new ArrayList<>(states.size());
        for (Future<long[]> state : states) relBatches.add(state.get());
        return relBatches;
    }

    private <R> R get(Future<R> future) {
        try {
            return future.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    private List<long[]> batchLongIterator(PrimitiveLongIterator nodes) {
        List<long[]> batches = new ArrayList<>(1024);
        while (nodes.hasNext()) {
            // todo reuse and hand back the long arrays
            long[] batch = new long[batchSize];
            int len = grabBatch(nodes, batch);
            // todo store node-id's for the next round, or actually better the relationship-records depending on memory requirements
            batches.add(batch);
        }
        return batches;
    }

    private int grabBatch(PrimitiveLongIterator nodes, long[] batch) {
        try (Transaction tx = api.beginTx()) {
            int idx = 0;
            while (nodes.hasNext() && idx < batchSize) {
                batch[idx++] = nodes.next();
            }
            if (idx < batchSize) batch[idx] = -1;
            tx.success();
            return idx;
        }
    }

    private <STATE, RESULT> Future<STATE> runRelBatch(PregelProgram<STATE, RESULT> program, long[] relBatch) {
        return pool.submit(() -> {
            try (Transaction tx = api.beginTx()) {
                Statement statement = ctx.get();
                STATE state = program.state();
                int len = relBatch.length;
                for (int idx = 0; idx< len; idx += 4) {
                    program.accept(relBatch[idx], relBatch[idx+1], relBatch[idx+2], (int)relBatch[idx+3], statement, state);
                }
                tx.success();
                return state;
            }
        });
    }

    private <STATE, RESULT> Future<STATE> runBatch(NodeExpander expander, PregelProgram<STATE, RESULT> program, long[] batch) {
        return pool.submit(() -> {
            try (Transaction tx = api.beginTx()) {
                Statement statement = ctx.get();
                STATE state = program.state();
                for (long node : batch) {
                    if (node == -1) break;
                    expander.expand(node, statement, (id, type, start, end) -> {
                        program.accept(id, start, end, type, statement, state);
                    });
                }
                tx.success();
                return state;
            }
        });
    }

    public Statement statement() {
        return ctx.get();
    }

    public interface NodeExpander {
        boolean expand(long node, Statement stmt, RelationshipVisitor<RuntimeException> callback);
    }

    public interface PregelProgram<STATE, RESULT> {
        boolean accept(long relId, long start, long end, int type, Statement stmt, STATE state);

        RESULT next(List<STATE> states);

        STATE state();

        PregelProgram<STATE, RESULT> newInstance();
    }

    public static class AllExpander implements NodeExpander {
        public boolean expand(long node, Statement stmt, RelationshipVisitor<RuntimeException> callback) {
            ReadOperations reads = stmt.readOperations();
            RelationshipIterator rels = relationships(node, reads);
            while (rels.hasNext()) {
                rels.relationshipVisit(rels.next(), callback);
            }
            return false;
        }

        private RelationshipIterator relationships(long node, ReadOperations reads) {
            try {
                return reads.nodeGetRelationships(node, Direction.BOTH);
            } catch (EntityNotFoundException e) {
                throw new RuntimeException("error expanding node " + node, e);
            }
        }
    }

    private class CollectingRelationshipVisitor implements RelationshipVisitor<RuntimeException> {
        int idx = 0;
        long[] relBatch = new long[batchSize * 10];

        public void visit(long id, int type, long start, long end) throws RuntimeException {
            if (idx + 4 > relBatch.length) relBatch = Arrays.copyOf(relBatch, relBatch.length + batchSize * 10);
            relBatch[idx] = id; relBatch[idx + 1] = start; relBatch[idx + 2] = end; relBatch[idx + 3] = type;
            idx += 4;
        }

        public long[] getRelBatch() {
            return Arrays.copyOf(relBatch, idx);
        }
    }
}
