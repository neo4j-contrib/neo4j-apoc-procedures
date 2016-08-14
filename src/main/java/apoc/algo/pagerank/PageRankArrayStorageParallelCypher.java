package apoc.algo.pagerank;

import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import javax.sound.midi.SysexMessage;
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
        int index = Arrays.binarySearch(nodeMapping, node);
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

    private void readDataIntoArray(String relCypher, String nodeCypher) {
        Result nodeResult = db.execute(nodeCypher);
        Result nodeCountResult = db.execute(nodeCypher);

        long before = System.currentTimeMillis();
        // TODO Do this lazily.
        int totalNodes = (int)nodeCountResult.stream().count();
        long after = System.currentTimeMillis();
        System.out.println("Time to count total nodes = " + (after - before) + " millis");
        this.nodeCount = totalNodes;
        nodeMapping = new int[totalNodes];
        sourceDegreeData = new int[totalNodes];
        sourceWeightData = new int[totalNodes];
        sourceChunkStartingIndex = new int[totalNodes];

        before = System.currentTimeMillis();
        ResourceIterator<Object> resultIterator = nodeResult.columnAs("id");
        int index = 0;
        while(resultIterator.hasNext()) {
            int node  = ((Long)resultIterator.next()).intValue();
            nodeMapping[index] = node;
            index++;
        }

        after = System.currentTimeMillis();
        System.out.println("Time to make nodes structure= " + (after - before) + " millis");

        before = System.currentTimeMillis();
        Arrays.sort(nodeMapping);
        after = System.currentTimeMillis();
        System.out.println("Time to sort nodes structure= " + (after - before) + " millis");
        pageRanks = new int [totalNodes];
        previousPageRanks = new int[totalNodes];
        pageRanksAtomic = new AtomicIntegerArray(totalNodes);

        Result result = db.execute(relCypher);
        int totalRelationships = 0;
        before = System.currentTimeMillis();
        while(result.hasNext()) {
            Map<String, Object> res = result.next();
            int source = ((Long) res.get("source")).intValue();
            int weight = ((Long) res.getOrDefault("weight", 1)).intValue();

            int sourceIndex = getNodeIndex(source);

            int storedDegree = sourceDegreeData[sourceIndex];
            if (storedDegree == -1) {
                sourceDegreeData[sourceIndex] = 1;
            } else {
                sourceDegreeData[sourceIndex] = storedDegree + 1;
            }

            int storedWeight = sourceWeightData[sourceIndex];
            if (storedWeight == -1) {
                sourceWeightData[sourceIndex] = weight;
            } else {
                sourceWeightData[sourceIndex] = weight + storedWeight;
            }

            totalRelationships++;
            if (totalRelationships%100000 == 0) {
                System.out.println("Processed " + totalRelationships);
            }
        }

        after = System.currentTimeMillis();
        System.out.println("Time for 1st iteration over " + totalRelationships + " relations = " + (after - before) + " millis");

        result.close();
        relationshipTarget = new int[totalRelationships];
        relationshipWeight = new int[totalRelationships];

        Arrays.fill(relationshipTarget, -1);
        Arrays.fill(relationshipWeight, -1);

        this.relCount = totalRelationships;
        before = System.currentTimeMillis();
        calculateChunkIndices();
        after = System.currentTimeMillis();
        System.out.println("Time calculating chunk indices = " + (after - before) + " millis");

        before = System.currentTimeMillis();
        // We have degrees for all the nodes at the point.
        int count = 0;
        result = db.execute(relCypher);
        while(result.hasNext()) {
            Map<String, Object> res = result.next();
            int source = ((Long) res.get("source")).intValue();
            int target = ((Long) res.get("target")).intValue();
            int weight = ((Long) res.getOrDefault("weight", 1)).intValue();

            int logicalSourceIndex = getNodeIndex(source);
            int logicalTargetIndex = getNodeIndex(target);
            int chunkIndex = sourceChunkStartingIndex[logicalSourceIndex];
            while(relationshipTarget[chunkIndex] != -1) {
                chunkIndex++;
            }
            relationshipTarget[chunkIndex] = logicalTargetIndex;
            relationshipWeight[chunkIndex] = weight;
            if (count % 100000 == 0) {
                System.out.println("2: Processed " + count + " rels");
            }
            count++;
        }

        after = System.currentTimeMillis();
        System.out.println("Time for second iteration over relations = " + (after - before) + " millis");
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
