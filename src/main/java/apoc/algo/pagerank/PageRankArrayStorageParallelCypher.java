package apoc.algo.pagerank;

import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicIntegerArray;

import static apoc.algo.pagerank.PageRankUtils.toFloat;
import static apoc.algo.pagerank.PageRankUtils.toInt;

public class PageRankArrayStorageParallelCypher implements PageRank
{
    public static final int ONE_MINUS_ALPHA_INT = toInt( ONE_MINUS_ALPHA );
    public static final int WRITE_BATCH=10_100;
    public static final int INITIAL_ARRAY_SIZE=100_000;
    public final int BATCH_SIZE = 10_000 ;
    private final GraphDatabaseAPI db;
    private final ExecutorService pool;
    private String nodeCypher;
    private String relCypher;
    private int nodeCount;
    private int relCount;


    /*
        1. Memory usage right now:
            6 arrays : size of nodes.
            2 arrays : size of relationships.
            Memory usage: 6*N + 2*M
        2. TODO:
        Run this in parallel using ExecutorService.
        Sort and divide into (number of threads) chunks and process it in parallel.
     */

    int [] nodeMapping;
    int [] sourceDegreeData;

    // Weighted Degrees.
    // Don't absolutely need following two. Can be calculated on the fly.
    int [] sourceWeightData;
    int [] sourceChunkStartingIndex;

    // Storing relationships
    int [] relationshipTarget;
    int [] relationshipWeight;

    // Output arrays.
    int [] pageRanks;
    int [] previousPageRanks;
    private AtomicIntegerArray pageRanksAtomic;

    public PageRankArrayStorageParallelCypher(
            GraphDatabaseAPI db,
            ExecutorService pool,
            String nodeCypher,
            String relCypher)
    {
        this.pool = pool;
        this.db = db;
        this.relCypher = relCypher;
        this.nodeCypher = nodeCypher;
        readDataIntoArray(this.relCypher, this.nodeCypher);
    }

    private int getNodeIndex(int node) {
        int index = Arrays.binarySearch(nodeMapping, 0, nodeCount, node);
        return index;
    }

    private void calculateChunkIndices() {
        int currentIndex = 0;
        for (int i = 0; i < nodeCount; i++) {
            sourceChunkStartingIndex[i] = currentIndex;
            if (sourceDegreeData[i] == -1)
                continue;
            currentIndex += sourceDegreeData[i];
        }
    }

    private int [] doubleSize(int [] array, int currentSize) {
        int newArray[] = new int[currentSize * 2];
        System.arraycopy(array, 0, newArray, 0, currentSize);
        return newArray;
    }

    private void readDataIntoArray(String relCypher, String nodeCypher) {
        Result nodeResult = db.execute(nodeCypher);

        long before = System.currentTimeMillis();
        ResourceIterator<Object> resultIterator = nodeResult.columnAs("id");
        int index = 0;
        int totalNodes = 0;
        nodeMapping = new int[INITIAL_ARRAY_SIZE];
        int currentSize = INITIAL_ARRAY_SIZE;
        int previousNode = -1;
        while(resultIterator.hasNext()) {
            if (index >= currentSize) {
                System.out.println("Node Doubling size " + currentSize);
                nodeMapping = doubleSize(nodeMapping, currentSize);
                currentSize = currentSize * 2;
            }
            int node  = ((Long)resultIterator.next()).intValue();
            nodeMapping[index] = node;
            if (previousNode >= node) {
                System.out.println("Exit.");
            }
            index++;
            totalNodes++;
        }

        this.nodeCount = totalNodes;
        sourceDegreeData = new int[totalNodes];
        sourceWeightData = new int[totalNodes];
        sourceChunkStartingIndex = new int[totalNodes];

        for (int i = 0; i < nodeCount; i++)
            System.out.println(i + " " + nodeMapping[i]);
        long after = System.currentTimeMillis();
        System.out.println("Time to make nodes structure= " + (after - before) + " millis");

        pageRanks = new int [totalNodes];
        previousPageRanks = new int[totalNodes];
        pageRanksAtomic = new AtomicIntegerArray(totalNodes);

        before = System.currentTimeMillis();
        Result result = db.execute(relCypher);
        after = System.currentTimeMillis();
        System.out.println("Time to execute cypher = " + (after - before) + " millis");

        int totalRelationships = 0;
        before = System.currentTimeMillis();
        int previousSource = -1;
        int currentChunkIndex = 0;
        int currentRelationSize = INITIAL_ARRAY_SIZE;
        relationshipTarget = new int[currentRelationSize];
        relationshipWeight = new int[currentRelationSize];
        Arrays.fill(sourceChunkStartingIndex, -1);
        Arrays.fill(relationshipTarget, -1);
        Arrays.fill(relationshipWeight, -1);

        while(result.hasNext()) {
            Map<String, Object> res = result.next();
            int source = ((Long) res.get("source")).intValue();
            int target = ((Long) res.get("target")).intValue();
            int weight = ((Long) res.getOrDefault("weight", 1)).intValue();
            if (source < previousSource) {
                System.out.println("Failure to read.");
                return;
            }

            int sourceIndex = getNodeIndex(source);
            int logicalTargetIndex = getNodeIndex(target);

            int storedDegree = sourceDegreeData[sourceIndex];
            if (storedDegree == 0) {
                sourceDegreeData[sourceIndex] = 1;
            } else {
                sourceDegreeData[sourceIndex] = storedDegree + 1;
            }

            if (sourceChunkStartingIndex[sourceIndex] == -1) {
                sourceChunkStartingIndex[sourceIndex] = currentChunkIndex;
            }

            int storedWeight = sourceWeightData[sourceIndex];
            if (storedWeight == -1) {
                sourceWeightData[sourceIndex] = weight;
            } else {
                sourceWeightData[sourceIndex] = weight + storedWeight;
            }

            // Add the relationships.
            if (totalRelationships >= currentRelationSize) {
                System.out.println("Doubling relationsize " + currentRelationSize);
                relationshipTarget = doubleSize(relationshipTarget, currentRelationSize);
                relationshipWeight = doubleSize(relationshipWeight, currentRelationSize);
                currentRelationSize = 2 * currentRelationSize;
            }

            relationshipTarget[currentChunkIndex] = logicalTargetIndex;
            relationshipWeight[currentChunkIndex] = weight;

            currentChunkIndex += 1;
            totalRelationships++;
            if (totalRelationships%100000 == 0) {
                System.out.println("Processed " + totalRelationships);
            }
        }

        after = System.currentTimeMillis();
        System.out.println("Time for iteration over " + totalRelationships + " relations = " + (after - before) + " millis");
        result.close();
        this.relCount = totalRelationships;
        result.close();
    }

    public void computeParallel(int iterations) {
        for (int iteration = 0; iteration < iterations; iteration++) {
            long before = System.currentTimeMillis();
            startIterationParallel();
            iterateParallel(iteration);
            long after = System.currentTimeMillis();
            System.out.println("Time for iteration " + iteration + "  " + (after - before) + " millis");
        }
    }

    private void printPagerank() {
        for (int i = 0; i < pageRanksAtomic.length(); i++) {
            System.out.println(i + " " + pageRanksAtomic.get(i));
        }
    }

    private void princhunk() {
        for (int i = 0; i < nodeCount; i++) {
            System.out.println(i + " " + sourceChunkStartingIndex[i]);
        }
    }

    @Override
    public void compute(
            int iterations,
            RelationshipType... relationshipTypes)
    {
        for (int iteration = 0; iteration < iterations; iteration++) {
            startIteration();
            iterate();
        }

        previousPageRanks = null;
        sourceDegreeData = null;
        sourceWeightData = null;
        sourceChunkStartingIndex = null;
        relationshipTarget = null;
        relationshipWeight = null;
    }

    private int getEndNode(int node) {
        int endNode = node;
        while(endNode < nodeCount &&
                (sourceChunkStartingIndex[endNode] - sourceChunkStartingIndex[node] <= BATCH_SIZE)) {
            endNode++;
        }

        return endNode;
    }
    private void iterateParallel(int iter) {
        int batches = (int)nodeCount/BATCH_SIZE;
        List<Future> futures = new ArrayList<>(batches);
        int nodeIter = 0;
        int batchNo = 0;

        while(nodeIter < nodeCount) {
            // Process BATCH_SIZE relationships in one batch, aligned to the chunksize.
            final int start = nodeIter;
            final int end = getEndNode(nodeIter);
            Future future = pool.submit(new Runnable() {
                @Override
                public void run() {
                    int relProcessed = 0;
                    for (int i = start; i < end; i++) {
                        int chunkIndex = sourceChunkStartingIndex[i];
                        int degree = sourceDegreeData[i];

                        for (int j = 0; j < degree; j++) {
                            int source = i;
                            int target = relationshipTarget[chunkIndex + j];
                            int weight = relationshipWeight[chunkIndex + j];
                            pageRanksAtomic.addAndGet(target, weight * previousPageRanks[source]);
                            relProcessed++;
                        }
                    }

                    if (iter == 0)
                    System.out.println(Thread.currentThread().getName() + " processed " + relProcessed);
                }
            });

            batchNo++;
            nodeIter = end;
            futures.add(future);
        }

        PageRankUtils.waitForTasks(futures);
    }

    private void iterate() {

        for (int i = 0; i < nodeCount; i++) {
            int chunkIndex = sourceChunkStartingIndex[i];
            int degree = sourceDegreeData[i];

            for (int j = 0; j < degree; j++) {
                int source = i;
                int target = relationshipTarget[chunkIndex + j];
                int weight = relationshipWeight[chunkIndex + j];

                int oldValue = pageRanks[target];
                int newValue =  oldValue + weight * previousPageRanks[source];
                pageRanks[target] = newValue;
            }
        }
    }

    private void startIteration()
    {
        for (int node = 0; node < nodeCount; node++) {
            int weightedDegree = sourceWeightData[node];

            if (weightedDegree == -1) {
                continue;
            }
            int prevRank = pageRanks[node];
            previousPageRanks[node] =  toInt(ALPHA * toFloat(prevRank) / weightedDegree);
            pageRanks[node] = ONE_MINUS_ALPHA_INT;
        }
    }

    private void startIterationParallel()
    {
        for (int node = 0; node < nodeCount; node++) {
            int weightedDegree = sourceWeightData[node];

            if (weightedDegree == -1) {
                continue;
            }
            int prevRank = pageRanksAtomic.get(node);
            previousPageRanks[node] =  toInt(ALPHA * toFloat(prevRank) / weightedDegree);
            pageRanksAtomic.set(node, ONE_MINUS_ALPHA_INT);
        }
    }

    public void writeResultsToDB() {
        PageRankUtils.writeBackResults(pool, db, nodeMapping, this, WRITE_BATCH);
    }


    public double getResultNonParallel(long node)
    {
        double val = 0;
        int logicalIndex = getNodeIndex((int)node);

        if (logicalIndex >= 0 && pageRanks.length >= logicalIndex) {
            val = toFloat(pageRanks[logicalIndex]);
        }
        return val;
    }

    public double getResult(long node)
    {
        double val = 0;
        int logicalIndex = getNodeIndex((int)node);

        if (logicalIndex >= 0 && pageRanksAtomic.length() >= logicalIndex) {
            val = toFloat(pageRanksAtomic.get(logicalIndex));
        }
        return val;
    }

    public String getPropertyName()
    {
        return "pagerank";
    }

    public long numberOfNodes() {
        return nodeCount;
    };

    public long numberOfRels(){
        return relCount;
    };
}
