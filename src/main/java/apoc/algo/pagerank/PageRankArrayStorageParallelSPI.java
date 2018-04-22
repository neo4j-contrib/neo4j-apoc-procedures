package apoc.algo.pagerank;

import apoc.algo.algorithms.AlgoUtils;
import apoc.algo.algorithms.AlgorithmInterface;
import apoc.stats.DegreeUtil;
import apoc.util.kernel.MultiThreadedGlobalGraphOperations;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.TerminationGuard;

import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicIntegerArray;

import static apoc.algo.pagerank.PageRankArrayStorageParallelCypher.WRITE_BATCH;
import static apoc.algo.pagerank.PageRankUtils.*;
import static apoc.util.kernel.MultiThreadedGlobalGraphOperations.GlobalOperationsTypes.NODES;
import static apoc.util.kernel.MultiThreadedGlobalGraphOperations.GlobalOperationsTypes.RELATIONSHIPS;

public class PageRankArrayStorageParallelSPI implements PageRank, AlgorithmInterface {

    public static final int ONE_MINUS_ALPHA_INT = toInt(ONE_MINUS_ALPHA);
    private final GraphDatabaseAPI db;
    private final TerminationGuard guard;
    private final int nodeCount;
    private final ExecutorService pool;
    private final int relCount;
    private final DependencyResolver dependencyResolver;
    private final KernelTransaction ktx;
    private AtomicIntegerArray dst;

    private PageRankStatistics stats = new PageRankStatistics();

    public PageRankArrayStorageParallelSPI(
            GraphDatabaseService db, KernelTransaction ktx,
            TerminationGuard guard, ExecutorService pool) {
        this.guard = guard;
        this.pool = pool;
        this.db = (GraphDatabaseAPI) db;
        this.ktx = ktx;
        this.dependencyResolver = ((GraphDatabaseAPI) db).getDependencyResolver();
        this.nodeCount = (int) MultiThreadedGlobalGraphOperations.getHighestIdInUseForStore(dependencyResolver, NODES);
        this.relCount = (int) MultiThreadedGlobalGraphOperations.getHighestIdInUseForStore(dependencyResolver, RELATIONSHIPS);
    }

    @Override
    public void compute(int iterations, RelationshipType... relationshipTypes) {
        stats.iterations = iterations;
        long start = System.currentTimeMillis();
        final int[] src = new int[nodeCount];
        dst = new AtomicIntegerArray(nodeCount);
        final int[] degrees = computeDegrees();
        stats.readNodeMillis = System.currentTimeMillis() - start;
        stats.nodes = nodeCount;
        start = System.currentTimeMillis();

        int[] relationshipTypesIds = fetchRelationshipTypeIds(relationshipTypes);

        stats.readRelationshipMillis = System.currentTimeMillis() - start;
        stats.relationships = relCount;

        start = System.currentTimeMillis();
        for (int iteration = 0; iteration < iterations; iteration++) {
            startIteration(src, dst, degrees);
            MultiThreadedGlobalGraphOperations.forAllRelationships(db, pool, BATCH_SIZE, (ktx, relationshipScanCursor) -> {
                if (relationshipTypes.length == 0 || contains(relationshipTypesIds, relationshipScanCursor.type())) {
                    int endNode = (int) relationshipScanCursor.targetNodeReference();
                    int startNode = (int) relationshipScanCursor.sourceNodeReference();
                    dst.addAndGet(endNode, src[startNode]);
                }
            });
        }
        stats.computeMillis = System.currentTimeMillis() - start;
    }

    private int[] fetchRelationshipTypeIds(RelationshipType[] relationshipTypes) {
        int[] result = new int[relationshipTypes.length];
        TokenRead tokenRead = ktx.tokenRead();
        for (int i=0; i<relationshipTypes.length; i++) {
            result[i] = tokenRead.relationshipType(relationshipTypes[i].name());
        }
        return result;
    }

    private boolean contains(int[] relationshipTypes, int type) {
        for (int i=0; i<relationshipTypes.length; i++) {
            if (relationshipTypes[i]==type) {
                return true;
            }
        }
        return false;
    }

    private void startIteration(int[] src, AtomicIntegerArray dst, int[] degrees) {
        for (int node = 0; node < this.nodeCount; node++) {
            if (degrees[node] == -1) {
                continue;
            }
            src[node] = toInt(ALPHA * toFloat(dst.getAndSet(node, ONE_MINUS_ALPHA_INT)) / degrees[node]);

        }
    }

    private int[] computeDegrees() {
        final int[] degree = new int[nodeCount];
        Arrays.fill(degree, -1);
        MultiThreadedGlobalGraphOperations.forAllNodes(db, pool, BATCH_SIZE, (ktx, nodeCursor) -> {
            degree[(int) nodeCursor.nodeReference()] = DegreeUtil.degree(nodeCursor, ktx.cursors(), -1, Direction.OUTGOING);
        });
        return degree;
    }

    public double getResult(long node) {
        return dst != null ? toFloat(dst.get((int) node)) : 0;
    }


    public long numberOfNodes() {
        return nodeCount;
    }

    public String getPropertyName() {
        return "pagerank";
    }

    @Override
    public PageRankStatistics getStatistics() {
        return stats;
    }

    @Override
    public long getMappedNode(int algoId) {
        return (int) algoId;
    }

    public void writeResultsToDB() {
        stats.write = true;
        long before = System.currentTimeMillis();
        AlgoUtils.writeBackResults(pool, db, this, WRITE_BATCH, guard);
        stats.write = true;
        stats.writeMillis = System.currentTimeMillis() - before;
        stats.property = getPropertyName();
    }


}

