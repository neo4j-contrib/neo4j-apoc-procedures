package apoc.algo.algorithms;

import apoc.Pools;
import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveIntObjectMap;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class BetweennessCentrality implements AlgorithmInterface {
    public static final int WRITE_BATCH=100_000;
    public final int MINIMUM_BATCH_SIZE =10_000 ;
    private Algorithm algorithm;
    private Log log;
    GraphDatabaseAPI db;
    ExecutorService pool;
    private int nodeCount;
    private int relCount;
    private Statistics stats = new Statistics();

    private PrimitiveIntObjectMap intermediateBcPerThread;
    float betweennessCentrality[];
    private String property;

    public BetweennessCentrality(GraphDatabaseAPI db,
                                 ExecutorService pool, Log log)
    {
        this.pool = pool;
        this.db = db;
        this.log = log;
        algorithm = new Algorithm(db, pool, log);
    }

    @Override
    public double getResult(long node) {
        float val = -1;
        int logicalIndex = algorithm.getAlgoNodeId((int)node);
        if (logicalIndex >= 0 && betweennessCentrality.length >= logicalIndex) {
            val = betweennessCentrality[logicalIndex];
        }
        return val;
    }

    @Override
    public long numberOfNodes() {
        return nodeCount;
    }

    @Override
    public String getPropertyName() {
        return "betweenness_centrality";
    }

    @Override
    public long getMappedNode(int algoId) {
        return algorithm.getMappedNode(algoId);
    }

    public boolean readNodeAndRelCypherData(String relCypher, String nodeCypher, Number weight, Number batchSize, int concurrency) {
        boolean success = algorithm.readNodeAndRelCypher(relCypher, nodeCypher,weight, batchSize, concurrency);
        this.nodeCount = algorithm.getNodeCount();
        this.relCount = algorithm.relCount;
        stats.readNodeMillis = algorithm.readNodeMillis;
        stats.readRelationshipMillis = algorithm.readRelationshipMillis;
        stats.nodes = nodeCount;
        stats.relationships = relCount;
        return success;
    }

    public long numberOfRels() {
        return relCount;
    }

    public Statistics getStatistics() {
        return stats;
    }

    public void computeUnweightedSeq() {
        computeUnweightedSeq(algorithm.sourceDegreeData,
                algorithm.sourceChunkStartingIndex,
                algorithm.relationshipTarget);
    }

    private void computeUnweightedSeq(int[] sourceDegreeData, int[] sourceChunkStartingIndex, int[] relationshipTarget) {
        betweennessCentrality = new float[nodeCount];
        Arrays.fill(betweennessCentrality, 0);
        long before = System.currentTimeMillis();
        int start = 0;
        int end = nodeCount;
        processNodesInBatch(-1, start, end, sourceDegreeData, sourceChunkStartingIndex, relationshipTarget);
        long after = System.currentTimeMillis();
        long difference = after - before;
        log.info("Computations took " + difference + " milliseconds");
        stats.computeMillis = difference;
    }

    public void computeUnweightedParallel() {
        computeUnweightedParallel(algorithm.sourceDegreeData,
                algorithm.sourceChunkStartingIndex,
                algorithm.relationshipTarget);
    }

    public void computeUnweightedParallel(int [] sourceDegreeData,
                                  int [] sourceChunkStartingIndex,
                                  int [] relationshipTarget) {
        betweennessCentrality = new float[nodeCount];
        Arrays.fill(betweennessCentrality, 0);
        long before = System.currentTimeMillis();

        int numOfThreads = Pools.getNoThreadsInDefaultPool();
        assert(numOfThreads != 0);
        int batchSize = (int)nodeCount/numOfThreads;
        int batches = 0;
        if (batchSize > 0)
            batches = (int)nodeCount/batchSize;

        if (batchSize < MINIMUM_BATCH_SIZE) {
            batches = 1;
            batchSize = nodeCount;
        }


        List<Future> futures = new ArrayList<>(batches);
        intermediateBcPerThread = Primitive.intObjectMap();
        int nodeIter = 0;
        int batchNumber = 0;
        while(nodeIter < nodeCount) {
            final int start = nodeIter;
            final int end = Integer.min(start + batchSize, nodeCount);
            final int threadBatchNo = batchNumber;
            Future future = pool.submit(new Runnable() {
                @Override
                public void run() {
                    processNodesInBatch(threadBatchNo, start, end, sourceDegreeData, sourceChunkStartingIndex, relationshipTarget);
                }
            });
            nodeIter = end;
            batchNumber++;
            futures.add(future);
        }
        log.info("Total batches: " + batchNumber);
        AlgoUtils.waitForTasks(futures);

        compileResults(batchNumber);

        long after = System.currentTimeMillis();
        long difference = after - before;
        log.info("Computations took " + difference + " milliseconds");
        stats.computeMillis = difference;
    }

    private void compileResults(int batchNumber) {
        for (int i = 0; i < nodeCount; i++) {
            float value = 0;
            Object batchValue = 0;
            for (int batch = 0; batch < batchNumber; batch++) {
                batchValue = ((PrimitiveIntObjectMap)intermediateBcPerThread.get(batch)).get(i);
                if (batchValue != null)
                    value += (float)batchValue;
            }
            betweennessCentrality[i] = value;
        }
    }

    private void processNodesInBatch(int threadBatchNo,
                                     int start,
                                     int end,
                                     int [] sourceDegreeData,
                                     int [] sourceChunkStartingIndex,
                                     int [] relationshipTarget) {
        Stack<Integer> stack = new Stack<>(); // S
        Queue<Integer> queue = new LinkedList<>();

        log.info("Thread: " + Thread.currentThread().getName() + " processing " + start + " " + end);
        // Map<Integer, ArrayList<Integer>>predecessors = new HashMap<Integer, ArrayList<Integer>>(); // Pw

        PrimitiveIntObjectMap predecessors = Primitive.intObjectMap();

        int numShortestPaths[] = new int [nodeCount]; // sigma
        int distance[] = new int[nodeCount]; // distance
        PrimitiveIntObjectMap map = Primitive.intObjectMap();
        float delta[] = new float[nodeCount];

        int processedNode = 0;
        for (int source = start; source < end; source++) {

            processedNode++;
            if (sourceDegreeData[source] == 0) {
                continue;
            }

            stack.clear();
            predecessors.clear();
            Arrays.fill(numShortestPaths, 0);
            numShortestPaths[source] = 1;
            Arrays.fill(distance, -1);
            distance[source] = 0;
            queue.clear();
            queue.add(source);
            Arrays.fill(delta, 0);
            while (!queue.isEmpty()) {
                int nodeDequeued = queue.remove();
                stack.push(nodeDequeued);

                // For each neighbour of dequeued.
                int chunkIndex = sourceChunkStartingIndex[nodeDequeued];
                int degree = sourceDegreeData[nodeDequeued];

                for (int j = 0; j < degree; j++) {
                    int target = relationshipTarget[chunkIndex + j];

                    if (distance[target] < 0) {
                        queue.add(target);
                        distance[target] = distance[nodeDequeued] + 1;
                    }

                    if (distance[target] == (distance[nodeDequeued] + 1)) {
                        numShortestPaths[target] = numShortestPaths[target] + numShortestPaths[nodeDequeued];
                        if (!predecessors.containsKey(target)) {
                            ArrayList<Integer> list = new ArrayList<Integer>();
                            predecessors.put(target, list);
                        }
                        ((ArrayList<Integer>)predecessors.get(target)).add(nodeDequeued);
                    }
                }
            }

            int poppedNode;
            double partialDependency;
            while (!stack.isEmpty()) {
                poppedNode = stack.pop();
                ArrayList<Integer> list = (ArrayList<Integer>)predecessors.get(poppedNode);

                for (int i = 0; list != null && i < list.size() ; i++) {
                    int node = list.get(i);
                    assert(numShortestPaths[poppedNode] != 0);
                    partialDependency = (numShortestPaths[node] / (double) numShortestPaths[poppedNode]);
                    partialDependency *= (1.0) + delta[poppedNode];
                    delta[node] += partialDependency;
                }
                if (poppedNode != source && delta[poppedNode] != 0.0) {
                    if (threadBatchNo == -1) {
                        betweennessCentrality[poppedNode] = betweennessCentrality[poppedNode] + delta[poppedNode];
                    } else {
                        Object storedValue = map.get(poppedNode);
                        if (storedValue != null)
                            map.put(poppedNode, ((float)storedValue) + delta[poppedNode]);
                        else
                            map.put(poppedNode, delta[poppedNode]);
                    }
                }
            }

            if (processedNode%10000 == 0) {
                log.debug("Thread: " + Thread.currentThread().getName() + " processed " + processedNode);
            }
        }

        intermediateBcPerThread.put(threadBatchNo, map);
        delta = null;
        numShortestPaths = null;
        stack = null;
        queue = null;
        distance = null;
        log.debug("Thread: " + Thread.currentThread().getName() + " Finishing " + processedNode);
    }

    public void writeResultsToDB(String property) {
        this.property = property;
        stats.write = true;
        long before = System.currentTimeMillis();
        AlgoUtils.writeBackResults(pool, db, this, WRITE_BATCH);
        stats.writeMillis = System.currentTimeMillis() - before;
        stats.property = getPropertyName();
    }
}
