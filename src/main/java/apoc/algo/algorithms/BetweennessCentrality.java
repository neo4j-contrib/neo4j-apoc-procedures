package apoc.algo.algorithms;

import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class BetweennessCentrality implements AlgorithmInterface {
    public static final int WRITE_BATCH=100_000;
    public final int BATCH_SIZE =100_000 ;
    private Algorithm algorithm;
    private Log log;
    GraphDatabaseAPI db;
    ExecutorService pool;
    private int nodeCount;
    private int relCount;
    private Statistics stats = new Statistics();

    float betweennessCentrality[];
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
        double val = -1;
        int logicalIndex = algorithm.getNodeIndex((int)node);
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
    public int getMappedNode(int index) {
        int node = algorithm.nodeMapping[index];
        return node;
    }

    public boolean readNodeAndRelCypherData(String relCypher, String nodeCypher) {
        boolean success = algorithm.readNodesAndRelCypherWeighted(relCypher, nodeCypher);
        this.nodeCount = algorithm.nodeCount;
        this.relCount = algorithm.relCount;
        stats.readNodeMillis = algorithm.readNodeMillis;
        stats.readRelationshipMillis = algorithm.readRelationshipMillis;
        stats.nodes = nodeCount;
        stats.relationships = relCount;
        return success;
    }

    public boolean readRelCypherData(String relCypher) {
        boolean success = algorithm.readRelCypherData(relCypher);
        return success;
    }

    public long numberOfRels() {
        return relCount;
    }

    public Statistics getStatistics() {
        return stats;
    }

    public void computeUnweightedInBatches() {
        computeUnweightedInBatches(algorithm.sourceDegreeData,
                algorithm.sourceChunkStartingIndex,
                algorithm.relationshipTarget);
    }

    public void computeUnweightedInBatches(int [] sourceDegreeData,
                                  int [] sourceChunkStartingIndex,
                                  int [] relationshipTarget) {
        betweennessCentrality = new float[nodeCount];
        Arrays.fill(betweennessCentrality, 0);
        long before = System.currentTimeMillis();

        int batches = (int)nodeCount/BATCH_SIZE;
        List<Future> futures = new ArrayList<>(batches);
        int nodeIter = 0;
        while(nodeIter < nodeCount) {
            final int start = nodeIter;
            final int end = Integer.min(start + BATCH_SIZE, nodeCount);
            Future future = pool.submit(new Runnable() {
                @Override
                public void run() {
                    processNodesInBatch(start, end, sourceDegreeData, sourceChunkStartingIndex, relationshipTarget);
                }
            });
            nodeIter = end;
            futures.add(future);
        }

        int threadsReturned = AlgoUtils.waitForTasks(futures);
        int threadsNo = Runtime.getRuntime().availableProcessors()*2;
        System.out.println("Threads returned " + threadsReturned + " " + threadsNo);
        long after = System.currentTimeMillis();
        long difference = after - before;
        log.info("Computations took " + difference + " milliseconds");
        stats.computeMillis = difference;
//        for(int i = 0; i < nodeCount; i++) {
//            log.info(i + " : " + betweennessCentrality[i]);
//            System.out.println(i + " : " + betweennessCentrality[i]);
//        }
    }

    private void processNodesInBatch(int start,
                                     int end,
                             int [] sourceDegreeData,
                             int [] sourceChunkStartingIndex,
                             int [] relationshipTarget) {
        Stack<Integer> stack = new Stack<>(); // S
        Queue<Integer> queue = new LinkedList<>();

        log.info("Thread: " + Thread.currentThread().getName() + " processing " + start +  " " + end);
//        System.out.println("Thread: " + Thread.currentThread().getName() + " processing " + start + " " + end);
        Map<Integer, ArrayList<Integer>>predecessors = new HashMap<Integer, ArrayList<Integer>>(); // Pw

        int numShortestPaths[] = new int [nodeCount]; // sigma
        int distance[] = new int[nodeCount]; // distance

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
            for (int i = 0; i < nodeCount; i++) {
                ArrayList<Integer> list = new ArrayList<Integer>();
                predecessors.put(i, list);
            }
            while (!queue.isEmpty()) {
                int nodeDequeued = queue.remove();
//                System.out.println("Pushing Node dequeued: " + nodeDequeued + " source: " + source);
                stack.push(nodeDequeued);

                // For each neighbour of dequeued.
                int chunkIndex = sourceChunkStartingIndex[nodeDequeued];
                int degree = sourceDegreeData[nodeDequeued];

                for (int j = 0; j < degree; j++) {
                    int target = relationshipTarget[chunkIndex + j];

                    if (distance[target] < 0) {
                        queue.add(target);
//                        System.out.println("Pushing " + target + "to queue");
                        distance[target] = distance[nodeDequeued] + 1;
                    }

                    if (distance[target] == (distance[nodeDequeued] + 1)) {
                        numShortestPaths[target] = numShortestPaths[target] + numShortestPaths[nodeDequeued];
//                        System.out.println("Changing sigma to " + numShortestPaths[target] + " " + nodeDequeued + " " + target);
                        ArrayList<Integer> list = predecessors.get(target);
                        list.add(nodeDequeued);
                        predecessors.put(target, list);
                    }
                }
            }

//                for (int i = 0; i < nodeCount; i++) {
//                    ArrayList<Integer> list = predecessors.get(i);
//                   System.out.print("Predecessors on sp from " + source + " to " + i + ":");
//                    for (int x = 0; x < list.size(); x++) {
//                        System.out.print("->" + list.get(x));
//                    }
//                    System.out.println();
//                    //System.out.println(i + " : " + distance[i]);
//                    System.out.println("Shortest path from " + i + " : " + numShortestPaths[i]);
//                }
//
            while (!stack.isEmpty()) {
                int poppedNode = stack.pop();
//                System.out.println("Popped " + poppedNode);
                ArrayList<Integer> list = predecessors.get(poppedNode);

                for (int i = 0; i < list.size(); i++) {
                    int node = (int) list.get(i);
                    double partialDependency = (numShortestPaths[node] / (double) numShortestPaths[poppedNode]);
                    partialDependency *= (1.0) + delta[poppedNode];
                    delta[node] += partialDependency;
                }
                if (poppedNode != source) {
                    // betweennessCentrality[poppedNode] = betweennessCentrality[poppedNode] + delta[poppedNode];
                    log.info("Thread "  + Thread.currentThread().getName() + "  " + poppedNode + " adding " +
                            delta[poppedNode]);
                    addToBetweenness(poppedNode, delta[poppedNode]);
                }
            }

            if (processedNode%1000 == 0) {
                log.info("Thread: " + Thread.currentThread().getName() + " processed " + processedNode);
            }

        }
        log.info("Thread: " + Thread.currentThread().getName() + " Processed " + processedNode);

    }

    private synchronized void addToBetweenness(int node, float delta) {
        betweennessCentrality[node] += delta;
    }

    public void writeResultsToDB() {
        stats.write = true;
        long before = System.currentTimeMillis();
        AlgoUtils.writeBackResults(pool, db, this, WRITE_BATCH);
        stats.writeMillis = System.currentTimeMillis() - before;
        stats.property = getPropertyName();

    }
}
