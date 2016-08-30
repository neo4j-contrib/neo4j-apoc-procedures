package apoc.algo.algorithms;

import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;

import java.util.*;
import java.util.concurrent.ExecutorService;

public class BetweennessCentrality implements AlgorithmInterface {
    public static final int WRITE_BATCH=100_100;
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
        int logicalIndex = getMappedNode((int)node);

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


    public long numberOfRels() {
        return relCount;
    }

    public Statistics getStatistics() {
        return stats;
    }


    public void computeUnweighted() {
        computeUnweighted(algorithm.sourceDegreeData,
                algorithm.sourceChunkStartingIndex,
                algorithm.relationshipTarget);
    }

    public void computeUnweighted(int [] sourceDegreeData,
                                  int [] sourceChunkStartingIndex,
                                  int [] relationshipTarget) {
        // TODO Convert this to float using the multiplication methods
        betweennessCentrality = new float[nodeCount];
        Arrays.fill(betweennessCentrality, 0);
        Stack<Integer> stack = new Stack<>(); // S
        Queue<Integer> queue = new LinkedList<>();

        Map<Integer, ArrayList<Integer>>predecessors = new HashMap<Integer, ArrayList<Integer>>(); // Pw
        int numShortestPaths[] = new int [nodeCount]; // sigma
        int distance[] = new int[nodeCount]; // distance

        long before = System.currentTimeMillis();
        // TODO Convert this to float using the multiplication methods
        float delta[] = new float[nodeCount];
        System.out.println("Total nodes " + nodeCount + " total rels : " + relCount);
        for (int source = 0; source < nodeCount; source++) {
//            System.out.println("Source is " + source);
            // Initializations:
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
            while(!queue.isEmpty()) {
                int nodeDequeued = queue.remove();
//                System.out.println("Pushing Node dequeued: " + nodeDequeued + " source: " + source);
                stack.push(nodeDequeued);

                // For each neighbour of dequeued.
                int chunkIndex = sourceChunkStartingIndex[nodeDequeued];
                int degree = sourceDegreeData[nodeDequeued];

                for (int j = 0; j < degree; j++) {
                    int target = relationshipTarget[chunkIndex + j];
//                    System.out.println("Edge between " + nodeDequeued + " : " + target);

                    if (distance[target] < 0) {
                        queue.add(target);
//                        System.out.println("Pushing " + target + "to queue");
                        distance[target] = distance[nodeDequeued] + 1;
                    }

                    if (distance[target] == (distance[nodeDequeued] + 1)){
                        numShortestPaths[target] = numShortestPaths[target] + numShortestPaths[nodeDequeued];
//                        System.out.println("Changing sigma to " + numShortestPaths[target] + " " + nodeDequeued + " " + target);
                        ArrayList<Integer> list = predecessors.get(target);
                        list.add(nodeDequeued);
                        predecessors.put(target, list);
                    }
                }
            }

                for (int i = 0; i < nodeCount; i++) {
                    ArrayList<Integer> list = predecessors.get(i);
//                    System.out.print("Predecessors on sp from " + source + " to " + i + ":");
                    for (int x = 0; x < list.size(); x++) {
//                        System.out.print("->" + list.get(x));
                    }
//                    System.out.println();
                    //System.out.println(i + " : " + distance[i]);
//                    System.out.println("Shortest path from " + i + " : " + numShortestPaths[i]);
                }

            while(!stack.isEmpty()) {
                int poppedNode = stack.pop();
//                System.out.println("Popped " + poppedNode);
                ArrayList<Integer> list = predecessors.get(poppedNode);

                for (int i = 0; i < list.size(); i++) {
                    int node = (int)list.get(i);
                    double partialDependency = (numShortestPaths[node]/(double)numShortestPaths[poppedNode]);
                    partialDependency *= (1.0) + delta[poppedNode];
                    delta[node] += partialDependency;
                    if (node == 6 || node == 3) {
//                        System.out.println("adding delta " + partialDependency + "  " + delta[node] + "  to "  + node + "  " + poppedNode);
                    }
                }
                if (poppedNode != source) {
//                    if (poppedNode == 0)
//                        System.out.println("Adding " + delta[poppedNode] + " to " + betweennessCentrality[poppedNode]);
                    betweennessCentrality[poppedNode] = betweennessCentrality[poppedNode] + delta[poppedNode];
//                    System.out.println("source not Popped " + poppedNode + " " + betweennessCentrality[poppedNode] + " - " +
//                            delta[poppedNode]);

                }

            }

        }

        long after = System.currentTimeMillis();
        long difference = after - before;
        log.info("Computations took " + difference + " milliseconds");
        stats.computeMillis = difference;
        for(int i = 0; i < nodeCount; i++) {
//            log.info(i + " : " + betweennessCentrality[i]);
//            System.out.println(i + " : " + toFloat(betweennessCentrality[i]));
        }
    }

    public void writeResultsToDB() {
        stats.write = true;
        long before = System.currentTimeMillis();
        AlgoUtils.writeBackResults(pool, db, this, WRITE_BATCH);
        stats.writeMillis = System.currentTimeMillis() - before;
        stats.property = getPropertyName();

    }
}
