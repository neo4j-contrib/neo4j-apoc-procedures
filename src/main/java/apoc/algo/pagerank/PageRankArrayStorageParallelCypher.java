package apoc.algo.pagerank;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Result;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.Arrays;

import static apoc.algo.pagerank.PageRankUtils.toFloat;
import static apoc.algo.pagerank.PageRankUtils.toInt;

public class PageRankArrayStorageParallelCypher implements PageRank
{
    public static final int ONE_MINUS_ALPHA_INT = toInt( ONE_MINUS_ALPHA );
    public final int BATCH_SIZE = 100_000;
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

    public PageRankArrayStorageParallelCypher(
            GraphDatabaseService db,
            ExecutorService pool,
            String nodeCypher,
            String relCypher)
    {
        this.pool = pool;
        this.db = (GraphDatabaseAPI) db;
        this.relCypher = relCypher;
        this.nodeCypher = nodeCypher;
        readDataIntoArray(this.relCypher, this.nodeCypher);
    }

    private int getNodeIndex(int node) {
        int index = Arrays.binarySearch(nodeMapping, node);
        return index;
    }

    private int getDegree(int node) {
        int degree = 0;
        int index = Arrays.binarySearch(nodeMapping, node);
        if (index >= 0) {
            degree = sourceDegreeData[index];
        }
        return degree;
    }

    private void setDegree(int node, int degree) {
        int index = Arrays.binarySearch(nodeMapping, node);
        sourceDegreeData[index] = degree;
    }

    private int getWeight(int node) {
        int degree = 0;
        int index = Arrays.binarySearch(nodeMapping, node);
        if (index >= 0) {
            degree = sourceWeightData[index];
        }
        return degree;
    }

    private void setWeight(int node, int weight) {
        int index = Arrays.binarySearch(nodeMapping, node);
        sourceWeightData[index] = weight;
    }

    private void calculateChunkIndices() {
        int currentIndex = 0;
        for (int i = 0; i < nodeCount; i++) {
            sourceChunkStartingIndex[i] = currentIndex;
            currentIndex += sourceDegreeData[i];
        }
    }

    private void readDataIntoArray(String relCypher, String nodeCypher) {
        Result nodeResult = db.execute(nodeCypher);
        Result nodeCountResult = db.execute(nodeCypher);

        // TODO Do this lazily.
        int totalNodes = (int)nodeCountResult.stream().count();
        this.nodeCount = totalNodes;
        nodeMapping = new int[totalNodes];
        sourceDegreeData = new int[totalNodes];
        sourceWeightData = new int[totalNodes];
        sourceChunkStartingIndex = new int[totalNodes];

        String columnName = nodeResult.columns().get(0);
        int index = 0;
        while(nodeResult.hasNext()) {
            Map<String, Object> res = nodeResult.next();
            int node = ((Long)res.get(columnName)).intValue();
            nodeMapping[index] = node;
            sourceDegreeData[index] = 0;
            sourceWeightData[index] = 0;
            index++;
        }

        Arrays.sort(nodeMapping);
        pageRanks = new int [totalNodes];
        previousPageRanks = new int[totalNodes];

        Result result = db.execute(relCypher);
        int totalRelationships = 0;
        while(result.hasNext()) {
            Map<String, Object> res = result.next();
            int source = ((Long) res.get("source")).intValue();
            int weight = ((Long) res.getOrDefault("weight", 1)).intValue();

            int storedDegree = getDegree(source);
            setDegree(source, storedDegree + 1);

            int storedWeight = getWeight(source);
            setWeight(source, storedWeight + weight);

            totalRelationships++;
        }

        result.close();
        relationshipTarget = new int[totalRelationships];
        relationshipWeight = new int[totalRelationships];

        for (int i = 0; i < totalRelationships; i++) {
            relationshipTarget[i] = -1;
        }
        this.relCount = totalRelationships;
        calculateChunkIndices();

        // We have degrees for all the nodes at the point.
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

            previousPageRanks[logicalSourceIndex] = 0;
            previousPageRanks[logicalTargetIndex] = 0;
            pageRanks[logicalTargetIndex] = 0;
        }

        result.close();
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

            if (weightedDegree == 0) {
                continue;
            }
            int prevRank = pageRanks[node];
            previousPageRanks[node] =  toInt(ALPHA * toFloat(prevRank) / weightedDegree);
            pageRanks[node] = ONE_MINUS_ALPHA_INT;

        }
    }

    // TODO Just write stuff.
    public void writeResultsBack() {

    }

    public double getResult(long node)
    {
        double val = 0;
        int logicalIndex = getNodeIndex((int)node);

        if (logicalIndex >= 0 && pageRanks.length >= logicalIndex) {
            val = toFloat(pageRanks[logicalIndex]);
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

}
