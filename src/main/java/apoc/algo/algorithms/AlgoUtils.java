package apoc.algo.algorithms;

import apoc.util.Util;
import org.neo4j.cypher.EntityNotFoundException;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.DataWriteOperations;
import org.neo4j.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.kernel.api.exceptions.explicitindex.AutoIndexingKernelException;
import org.neo4j.kernel.api.exceptions.schema.IllegalTokenNameException;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.values.storable.DoubleValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;
import org.neo4j.procedure.TerminationGuard;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class AlgoUtils {
    public static final String SETTING_CYPHER_NODE = "node_cypher";
    public static final String SETTING_CYPHER_REL = "rel_cypher";
    public static final String SETTING_WRITE = "write";
    public static final String SETTING_WEIGHTED = "weight";
    public static final String SETTING_BATCH_SIZE = "batchSize";

    public static final String DEFAULT_CYPHER_REL =
            "MATCH (s)-[r]->(t) RETURN id(s) as source, id(t) as target, 1 as weight";
    public static final String DEFAULT_CYPHER_NODE =
            "MATCH (s) RETURN id(s) as id";

    public static final boolean DEFAULT_PAGE_RANK_WRITE = false;

    public static String getCypher(Map<String, Object> config, String setting, String defaultString) {
        String cypher = (String) config.getOrDefault(setting, defaultString);
        if ("none".equals(cypher)) return null;
        if (cypher == null || cypher.isEmpty()) {
            return defaultString;
        }
        return cypher;
    }

    public static int waitForTasks(List<Future> futures) {
        int total = 0;
        for (Future future : futures) {
            try {
                future.get();
                total++;
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }
        futures.clear();
        return total;
    }

    public static void writeBackResults(ExecutorService pool, GraphDatabaseAPI db, AlgorithmInterface algorithm,
                                        int batchSize, TerminationGuard guard) {
        ThreadToStatementContextBridge ctx = db.getDependencyResolver().resolveDependency(ThreadToStatementContextBridge.class);
        int propertyNameId;
        try (Transaction tx = db.beginTx()) {
            // If the transaction is terminated just return
            if (Util.transactionIsTerminated(guard)) {
                return;
            }
            propertyNameId = ctx.get().tokenWriteOperations().propertyKeyGetOrCreateForName(algorithm.getPropertyName());
            tx.success();
        } catch (IllegalTokenNameException e) {
            throw new RuntimeException(e);
        }
        final long totalNodes = algorithm.numberOfNodes();
        int batches = (int) totalNodes / batchSize;
        List<Future> futures = new ArrayList<>(batches);
        for (int i = 0; i < totalNodes; i += batchSize) {
            int nodeIndex = i;
            final int start = nodeIndex;
            Future future = pool.submit(new Runnable() {
                public void run() {
                    try (Transaction tx = db.beginTx()) {
                        // If the transaction is terminated just return
                        if (Util.transactionIsTerminated(guard)) {
                            return;
                        }

                        DataWriteOperations ops = ctx.get().dataWriteOperations();
                        for (int i = 0; i < batchSize; i++) {
                            int nodeIndex = i + start;
                            if (nodeIndex >= totalNodes) break;

                            long graphNode = algorithm.getMappedNode(nodeIndex);
                            double value = algorithm.getResult(graphNode);
                            if (graphNode == -1) {
                                System.out.println("Node node found for " + graphNode + " mapped node " + nodeIndex);
                            } else
                                ops.nodeSetProperty(graphNode, propertyNameId, Values.doubleValue(value));
                        }
                        tx.success();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });

            futures.add(future);
        }
        AlgoUtils.waitForTasks(futures);
    }
}
