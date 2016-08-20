package apoc.algo.pagerank;

import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;

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
    public static final int WRITE_BATCH=100_100;
    public static final int INITIAL_ARRAY_SIZE=100_000;
    public final int BATCH_SIZE = 100_000 ;
    private final GraphDatabaseAPI db;
    private final Log log;
    private final ExecutorService pool;
    private int nodeCount;
    private int relCount;

    private PageRankStatistics stats = new PageRankStatistics();

    /*
        1. Memory usage right now:
            5 arrays : size of nodes.
            2 arrays : size of relationships.
            Memory usage: 4*N + 2*M
     */

    int [] sourceChunkStartingIndex;

    int [] nodeMapping;
    int [] sourceDegreeData;

    // Storing relationships
    int [] relationshipTarget;
    int [] relationshipWeight;

    // Output arrays.
    int [] previousPageRanks;
    private AtomicIntegerArray pageRanksAtomic;

    public PageRankArrayStorageParallelCypher(
            GraphDatabaseAPI db,
            ExecutorService pool, Log log)
    {
        this.pool = pool;
        this.db = db;
        this.log = log;
    }

    private int getNodeIndex(int node) {
        int index = Arrays.binarySearch(nodeMapping, 0, nodeCount, node);
        return index;
    }

    // TODO Create buckets instead of copying data.
    // Not doing it right now because of the complications of the interface.
    private int [] doubleSize(int [] array, int currentSize) {
        int newArray[] = new int[currentSize * 2];
        System.arraycopy(array, 0, newArray, 0, currentSize);
        return newArray;
    }

    public boolean readDataIntoArray(String relCypher, String nodeCypher) {
        Result nodeResult = db.execute(nodeCypher);

        long before = System.currentTimeMillis();
        ResourceIterator<Object> resultIterator = nodeResult.columnAs("id");
        int index = 0;
        int totalNodes = 0;
        nodeMapping = new int[INITIAL_ARRAY_SIZE];
        int currentSize = INITIAL_ARRAY_SIZE;
        while(resultIterator.hasNext()) {
            int node  = ((Long)resultIterator.next()).intValue();

            if (index >= currentSize) {
                if (log.isDebugEnabled()) log.debug("Node Doubling size " + currentSize);
                nodeMapping = doubleSize(nodeMapping, currentSize);
                currentSize = currentSize * 2;
            }
            nodeMapping[index] = node;
            index++;
            totalNodes++;
        }

        this.nodeCount = totalNodes;
        Arrays.sort(nodeMapping, 0, nodeCount);
        long after = System.currentTimeMillis();
        stats.readNodeMillis = (after - before);
        stats.nodes = totalNodes;
        log.info("Time to make nodes structure = " + stats.readNodeMillis + " millis");
        before = System.currentTimeMillis();

        sourceDegreeData = new int[totalNodes];
        previousPageRanks = new int[totalNodes];
        pageRanksAtomic = new AtomicIntegerArray(totalNodes);
        sourceChunkStartingIndex = new int[totalNodes];
        Arrays.fill(sourceChunkStartingIndex, -1);

        int totalRelationships = readRelationshipMetadata(relCypher);
        this.relCount = totalRelationships;
        relationshipTarget = new int[totalRelationships];
        relationshipWeight = new int[totalRelationships];
        Arrays.fill(relationshipTarget, -1);
        Arrays.fill(relationshipWeight, -1);
        calculateChunkIndices();
        readRelationships(relCypher, totalRelationships);
        after = System.currentTimeMillis();
        stats.relationships = totalRelationships;
        stats.readRelationshipMillis = (after - before);
        log.info("Time for iteration over " + totalRelationships + " relations = " + stats.readRelationshipMillis + " millis");
        return true;
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

    private int readRelationshipMetadata(String relCypher) {
        long before = System.currentTimeMillis();
        Result result = db.execute(relCypher);
        int totalRelationships = 0;
        int sourceIndex = 0;
        while(result.hasNext()) {
            Map<String, Object> res = result.next();
            int source = ((Long) res.get("source")).intValue();
            sourceIndex = getNodeIndex(source);

            sourceDegreeData[sourceIndex]++;
            totalRelationships++;
        }
        result.close();
        long after = System.currentTimeMillis();
        log.info("Time to read relationship metadata " + (after - before) + " ms");
        return totalRelationships;
    }

    private void readRelationships(String relCypher, int totalRelationships) {
        Result result = db.execute(relCypher);
        long before = System.currentTimeMillis();
        int sourceIndex = 0;
        while(result.hasNext()) {
            Map<String, Object> res = result.next();
            int source = ((Long) res.get("source")).intValue();
            sourceIndex = getNodeIndex(source);
            int target = ((Long) res.get("target")).intValue();
            int weight = ((Long) res.getOrDefault("weight", 1)).intValue();
            int logicalTargetIndex = getNodeIndex(target);
            int chunkIndex = sourceChunkStartingIndex[sourceIndex];
            while(relationshipTarget[chunkIndex] != -1) {
                chunkIndex++;
            }
            relationshipTarget[chunkIndex] = logicalTargetIndex;
            relationshipWeight[chunkIndex] = weight;
        }
        result.close();
        long after = System.currentTimeMillis();
        log.info("Time to read relationship data " + (after - before) + " ms");
    }

    @Override
    public void compute(int iterations, RelationshipType... relationshipTypes) {
        stats.iterations = iterations;
        long before = System.currentTimeMillis();

        for (int iteration = 0; iteration < iterations; iteration++) {
            long beforeIteration = System.currentTimeMillis();
            startIteration();
            iterateParallel(iteration);
            long afterIteration = System.currentTimeMillis();
            log.info("Time for iteration " + iteration + "  " + (afterIteration - beforeIteration) + " millis");
        }
        long after = System.currentTimeMillis();
        stats.computeMillis = (after - before);
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
        while(nodeIter < nodeCount) {
            // Process BATCH_SIZE relationships in one batch, aligned to the chunksize.
            final int start = nodeIter;
            final int end = getEndNode(nodeIter);
            Future future = pool.submit(new Runnable() {
                @Override
                public void run() {
                    for (int i = start; i < end; i++) {
                        int chunkIndex = sourceChunkStartingIndex[i];
                        int degree = sourceDegreeData[i];

                        for (int j = 0; j < degree; j++) {
                            int source = i;
                            int target = relationshipTarget[chunkIndex + j];
                            int weight = relationshipWeight[chunkIndex + j];
                            pageRanksAtomic.addAndGet(target, weight * previousPageRanks[source]);
                        }
                    }
                }
            });
            nodeIter = end;
            futures.add(future);
        }

        PageRankUtils.waitForTasks(futures);
    }

    private int getTotalWeightForNode(int node) {
        int chunkIndex = sourceChunkStartingIndex[node];
        int degree = sourceDegreeData[node];
        int totalWeight = 0;
        for (int i = 0; i < degree; i++) {
            totalWeight += relationshipWeight[chunkIndex + i];
        }
        return totalWeight;
    }

    private void startIteration()
    {
        for (int node = 0; node < nodeCount; node++) {
            int weightedDegree = getTotalWeightForNode(node);

            if (weightedDegree == -1) {
                continue;
            }
            int prevRank = pageRanksAtomic.get(node);
            previousPageRanks[node] =  toInt(ALPHA * toFloat(prevRank) / weightedDegree);
            pageRanksAtomic.set(node, ONE_MINUS_ALPHA_INT);
        }
    }

    public void writeResultsToDB() {
        stats.write = true;
        long before = System.currentTimeMillis();
        PageRankUtils.writeBackResults(pool, db, nodeMapping, this, WRITE_BATCH);
        stats.writeMillis = System.currentTimeMillis() - before;
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

    @Override
    public PageRankStatistics getStatistics() {
        return stats;
    }
}
