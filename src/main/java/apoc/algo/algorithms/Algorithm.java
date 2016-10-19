package apoc.algo.algorithms;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveLongIntMap;
import org.neo4j.collection.primitive.PrimitiveLongIntVisitor;
import org.neo4j.graphdb.Result;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;

import java.util.Arrays;
import java.util.concurrent.ExecutorService;

import static java.lang.System.currentTimeMillis;

public class Algorithm {

    public static final int INITIAL_ARRAY_SIZE=100_000;
    public static final String COMPILED_RUNTIME = "CYPHER runtime=compiled ";
    private final GraphDatabaseAPI db;
    private final Log log;
    private final ExecutorService pool;

    public int nodeCount;
    public int maxAlgoNodeId;
    public int relCount;
    public long readNodeMillis, readRelationshipMillis;

    // Arrays to hold the graph.
    // Mapping from Algo node ID to Graph nodeID
    private  int [] nodeMapping;
    private PrimitiveLongIntMap nodeMap = Primitive.longIntMap(INITIAL_ARRAY_SIZE);

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
        long before = currentTimeMillis();
        this.nodeCount = loadNodes(nodeCypher);
        readNodeMillis = (currentTimeMillis() - before);
        log.info("Time to load nodes = " + readNodeMillis + " millis. Nodes from nodeCypher: " + nodeCount);

        this.maxAlgoNodeId = nodeCount;

        before = currentTimeMillis();
        this.relCount = loadRelationships(relCypher, true,1);
        readRelationshipMillis = (currentTimeMillis() -before);
        log.info("Time for iteration over " + relCount + " relations = " + readRelationshipMillis + " millis");

        nodeMapping = new int[nodeCount];
        nodeMap.visitEntries(new PrimitiveLongIntVisitor<RuntimeException>() {
            @Override
            public boolean visited(long nodeId, int algoId) throws RuntimeException {
                nodeMapping[algoId]=(int)nodeId;
                return false;
            }
        });
        return true;
    }

    public int loadNodes(String nodeCypher) {
        if (nodeCypher == null) return  0;

        NodeLoaderVisitor loader = new NodeLoaderVisitor();
        db.execute(COMPILED_RUNTIME + nodeCypher).accept(loader);
        return loader.nodes;
    }

    public int getAlgoNodeId(long node) {
        return nodeMap.get(node); // Arrays.binarySearch(nodeMapping, 0, nodeCount, node);
    }

    private int getOrCreateAlgoNodeId(long node) {
        if (nodeMap.containsKey(node)) {
            return nodeMap.get(node);
        }
        nodeMap.put(node,maxAlgoNodeId++);
        return maxAlgoNodeId-1;
    }


    // TODO Create buckets instead of copying data.
    // Not doing it right now because of the complications of the interface.
    private int [] growArray(int[] array, float growFactor) {
        int currentSize = array.length;
        int newArray[] = new int[(int)(currentSize * growFactor)];
        System.arraycopy(array, 0, newArray, 0, currentSize);
        return newArray;
    }

    private void calculateChunkIndices() {
        int nodes = this.nodeCount;
        sourceChunkStartingIndex = new int[nodes];
        int currentOffset = 0;
        for (int i = 0; i < nodes; i++) {
            sourceChunkStartingIndex[i] = currentOffset;
            currentOffset += sourceDegreeData[i];
        }
    }

    private int loadRelationships(String relCypher, boolean weighted, int defaultWeight) {
        RelationshipLoaderVisitor loader = new RelationshipLoaderVisitor(weighted, defaultWeight, nodeCount);
        db.execute(COMPILED_RUNTIME +relCypher).accept(loader);

        this.nodeCount = maxAlgoNodeId;
        calculateChunkIndices();

        loader.transformRelData();
        return loader.totalRelationships;
    }

    public long getMappedNode(int algoId) {
        return nodeMapping[algoId];
    }

    private class NodeLoaderVisitor implements Result.ResultVisitor<RuntimeException> {
        int nodes = 0;

        @Override
        public boolean visit(Result.ResultRow row) throws RuntimeException {
            nodeMap.put(row.getNumber("id").longValue(),nodes);
            nodes++;
            return true;
        }
    }

    private class RelationshipLoaderVisitor implements Result.ResultVisitor<RuntimeException> {
        private final boolean weighted;
        private final int defaultWeight;
        int totalRelationships = 0;

        int sourceIndex = 0;
        int targetIndex = 0;
        long lastSource = -1, lastTarget = -1;

        int relDataIdx = 0;
        int[] relData;
        int relDataSize;

        PrimitiveLongIntMap weights;
        private int sourceDegreeSize;

        public RelationshipLoaderVisitor(boolean weighted, int defaultWeight, int nodeCount) {
            this.weighted = weighted;
            this.defaultWeight = defaultWeight;

            relData = new int[Math.max(nodeCount,INITIAL_ARRAY_SIZE)];
            relDataSize = relData.length;

            sourceDegreeData = new int[Math.max(nodeCount,INITIAL_ARRAY_SIZE)];
            sourceDegreeSize = sourceDegreeData.length;

            if (weighted) {
                weights = Primitive.longIntMap(INITIAL_ARRAY_SIZE);
            }
        }

        @Override
        public boolean visit(Result.ResultRow res) throws RuntimeException {
            totalRelationships++;

            if (relDataIdx+1 >= relDataSize) {
                relData = growArray(relData, 2f);
                relDataSize = relData.length;
            }

            long source = ((Number) res.get("source")).longValue();
            if (lastSource != source) {
                sourceIndex = getOrCreateAlgoNodeId(source);
                relData[relDataIdx++] = -sourceIndex - 1;
                lastSource = source;
            }

            long target = ((Number) res.get("target")).longValue();
            if (lastTarget != target) {
                targetIndex = getOrCreateAlgoNodeId(target);
                lastTarget = target;
            }
            if (weighted) {
                Number weightValue = res.getNumber("weight");
                if (weightValue != null) {
                    int weight = weightValue.intValue();
                    if (weight != defaultWeight) {
                        long key = ((long) sourceIndex) << 32 | targetIndex;
                        weights.put(key,weight);
                    }
                }
            }
            relData[relDataIdx++]=targetIndex;
            if (sourceIndex>=sourceDegreeSize) {
                sourceDegreeData = growArray(sourceDegreeData, 1.2f);
                sourceDegreeSize = sourceDegreeData.length;
            }
            sourceDegreeData[sourceIndex]++;
            return true;
        }

        private void transformRelData() {
            if (weighted) {
                transformRelationshipDataToOffsetStorage(totalRelationships, relData, relDataIdx, weights, defaultWeight);
            } else {
                transformRelationshipDataToOffsetStorage(totalRelationships, relData, relDataIdx);
            }
        }

        private void transformRelationshipDataToOffsetStorage(int totalRelationships, int[] relData, int relDataIdx, PrimitiveLongIntMap weights, int defaultWeight) {
            int sourceIndex = 0;
            int[] offsetTracker = Arrays.copyOf(sourceChunkStartingIndex,sourceChunkStartingIndex.length);

            relationshipTarget = new int[totalRelationships];
            relationshipWeight = new int[totalRelationships];
            if (defaultWeight!=0) Arrays.fill(relationshipWeight,defaultWeight);
            for (int i=0;i<relDataIdx;i++) {
                int id = relData[i];
                if (id < 0) {
                    sourceIndex = -id-1;
                } else {
                    long key = ((long) sourceIndex) << 32 | id;
                    if (weights.containsKey(key)) {
                        relationshipWeight[offsetTracker[sourceIndex]]=weights.get(key);
                    }
                    relationshipTarget[offsetTracker[sourceIndex]]=id;
                    offsetTracker[sourceIndex]++;
                }
            }
        }
        private void transformRelationshipDataToOffsetStorage(int totalRelationships, int[] relData, int relDataIdx) {
            int sourceIndex = 0;
            int[] offsetTracker = Arrays.copyOf(sourceChunkStartingIndex,sourceChunkStartingIndex.length);

            relationshipTarget = new int[totalRelationships];
            for (int i=0;i<relDataIdx;i++) {
                int id = relData[i];
                if (id < 0) {
                    sourceIndex = -id-1;
                } else {
                    relationshipTarget[offsetTracker[sourceIndex]++]=id;
                }
            }
        }
    }
}
