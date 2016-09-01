package apoc.algo.algorithms;

import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ExecutorService;

public class Algorithm {

    public static final int INITIAL_ARRAY_SIZE=100_000;
    private final GraphDatabaseAPI db;
    private final Log log;
    private final ExecutorService pool;

    public int nodeCount;
    public int relCount;
    public long readNodeMillis, readRelationshipMillis;

    // Arrays to hold the graph.
    // Mapping from Algo node ID to Graph nodeID
    public int [] nodeMapping;

    // Degree of each algo node.
    public int [] sourceDegreeData;

    // Starting index in relationship arrays for each algo node.
    public int [] sourceChunkStartingIndex;

    // Storing relationships
    public int [] relationshipTarget;
    public int [] relationshipWeight;

    public Algorithm(GraphDatabaseAPI db,
                     ExecutorService pool,
                     Log log) {
        this.pool = pool;
        this.db = db;
        this.log = log;
    }

    public boolean readNodesAndRelCypherWeighted(String relCypher, String nodeCypher) {
        return readNodeAndRelCypherIntoArrays(relCypher, nodeCypher, true);
    }

    public boolean readNodeAndRelCypherIntoArrays(String relCypher, String nodeCypher, boolean weighted) {
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
        readNodeMillis = (after - before);
        log.info("Time to make nodes structure = " + readNodeMillis + " millis");
        before = System.currentTimeMillis();

        sourceDegreeData = new int[totalNodes];
        sourceChunkStartingIndex = new int[totalNodes];
        Arrays.fill(sourceChunkStartingIndex, -1);

        int totalRelationships = readRelationshipMetadata(relCypher);
        this.relCount = totalRelationships;
        relationshipTarget = new int[totalRelationships];
        Arrays.fill(relationshipTarget, -1);

        if (weighted) {
            relationshipWeight = new int[totalRelationships];
            Arrays.fill(relationshipWeight, -1);
        }
        calculateChunkIndices();
        readRelationships(relCypher, weighted);
        after = System.currentTimeMillis();
        readRelationshipMillis = (after - before);
        log.info("Time for iteration over " + totalRelationships + " relations = " + readRelationshipMillis + " millis");
        return true;
    }

    public int getNodeIndex(int node) {
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

    private void readRelationships(String relCypher, boolean weighted) {
        Result result = db.execute(relCypher);
        long before = System.currentTimeMillis();
        int sourceIndex = 0;
        while(result.hasNext()) {
            Map<String, Object> res = result.next();
            int source = ((Long) res.get("source")).intValue();
            sourceIndex = getNodeIndex(source);
            int target = ((Long) res.get("target")).intValue();
            int logicalTargetIndex = getNodeIndex(target);
            int chunkIndex = sourceChunkStartingIndex[sourceIndex];
            while(relationshipTarget[chunkIndex] != -1) {
                chunkIndex++;
            }
            relationshipTarget[chunkIndex] = logicalTargetIndex;
            int weight = 0;
            if (weighted) {
                weight = ((Long) res.getOrDefault("weight", 1)).intValue();
                relationshipWeight[chunkIndex] = weight;
            }
        }
        result.close();
        long after = System.currentTimeMillis();
        log.info("Time to read relationship data " + (after - before) + " ms");
    }

    private boolean readRelationshipMetadataAndNodes(String relCypher, String nodeCypher, boolean weighted) {
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
        readNodeMillis = (after - before);
        log.info("Time to make nodes structure = " + readNodeMillis + " millis");
        before = System.currentTimeMillis();

        sourceDegreeData = new int[totalNodes];

        int totalRelationships = readRelationshipMetadata(relCypher);

        // At this point we have mapping and the degrees of nodes.
        this.relCount = totalRelationships;
        relationshipTarget = new int[totalRelationships];
        Arrays.fill(relationshipTarget, -1);

        if (weighted) {
            relationshipWeight = new int[totalRelationships];
            Arrays.fill(relationshipWeight, -1);
        }
        sourceChunkStartingIndex = new int[totalNodes];
        Arrays.fill(sourceChunkStartingIndex, -1);

        calculateChunkIndices();
        readRelationships(relCypher, weighted);
        after = System.currentTimeMillis();
        readRelationshipMillis = (after - before);
        log.info("Time for iteration over " + totalRelationships + " relations = " + readRelationshipMillis + " millis");
        return true;
    }

    private void readNodeAndSource(String relCypher) {
        long before = System.currentTimeMillis();
        Result result = db.execute(relCypher);
        int totalRelationships = 0;
        int sourceIndex = 0;
        int index = 0;
        int totalNodes = 0;
        nodeMapping = new int[INITIAL_ARRAY_SIZE];
        int currentSize = INITIAL_ARRAY_SIZE;
        while(result.hasNext()) {
            Map<String, Object> res = result.next();
            int source = ((Long) res.get("source")).intValue();
            int target = ((Long) res.get("target")).intValue();

            if (index >= currentSize) {
                if (log.isDebugEnabled()) log.debug("Node Doubling size " + currentSize);
                nodeMapping = doubleSize(nodeMapping, currentSize);
                currentSize = currentSize * 2;
            }

            // Don't map if it's already mapped.
            if (getNodeIndex(source) < 0) {
                nodeMapping[source] = source;
                sourceIndex = index;
                index++;
                totalNodes++;
            }

            if (getNodeIndex(target) < 0) {
                nodeMapping[target] = target;
                index++;
                totalNodes++;
            }

            sourceDegreeData[sourceIndex]++;
        }
        result.close();

        // We now have unsorted node mapping and corresponding degrees.
        int[] tempDegreeData = new int[totalNodes];

        long after = System.currentTimeMillis();
        log.info("Time to read relationship metadata and nodes" + (after - before) + " ms");

    }
    public boolean readRelCypherData(String relCypher) {

        return false;
    }
}
