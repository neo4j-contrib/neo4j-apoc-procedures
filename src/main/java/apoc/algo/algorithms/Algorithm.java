package apoc.algo.algorithms;

import apoc.Pools;
import apoc.util.Util;
import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveLongIntMap;
import org.neo4j.collection.primitive.PrimitiveLongIntVisitor;
import org.neo4j.graphdb.Result;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static apoc.util.Util.map;
import static java.lang.System.currentTimeMillis;

// todo optimize for only storing source nodes
// make sure the algorithms honor if a node was no source node, i.e. we have no outgoing relationship-information
// todo use pools

public class Algorithm implements AlgoIdGenerator {

    public static final int INITIAL_ARRAY_SIZE=100_000;
    public static final String COMPILED_RUNTIME = "CYPHER runtime=compiled ";
    private final GraphDatabaseAPI db;
    private final Log log;
    private final ExecutorService pool;

    public AtomicInteger maxAlgoNodeId = new AtomicInteger();
    public int relCount;
    public long readNodeMillis, readRelationshipMillis;

    // Arrays to hold the graph.
    // Mapping from Algo node ID to Graph nodeID
    private  int [] nodeMapping;
    private final PrimitiveLongIntMap nodeMap = Primitive.longIntMap(INITIAL_ARRAY_SIZE);

    // Degree of each algo node.
    public int [] sourceDegreeData;

    // Starting index in relationship arrays for each algo node.
    public int [] sourceChunkStartingIndex;

    // Storing relationships
    public int [] relationshipTarget;
    public int [] relationshipWeight;
    private Number batchSize;

    public Algorithm(GraphDatabaseAPI db,
                     ExecutorService pool,
                     Log log) {
        this.pool = pool;
        this.db = db;
        this.log = log;
    }

    public boolean readNodeAndRelCypher(String relCypher, String nodeCypher, Number weight, Number batchSize) {
        this.batchSize = batchSize;
        long before = currentTimeMillis();
        this.maxAlgoNodeId.set(loadNodes(nodeCypher));
        readNodeMillis = (currentTimeMillis() - before);
        log.info("Time to load nodes = " + readNodeMillis + " millis. Nodes from nodeCypher: " + getNodeCount());


        before = currentTimeMillis();
        if (batchSize == null) {
            this.relCount = loadRelationships(relCypher, weight != null, weight != null ? weight.intValue() : 1);
        } else {
            this.relCount = loadRelationshipsBatch(relCypher, weight != null, weight != null ? weight.intValue() : 1, batchSize.intValue());
        }
        readRelationshipMillis = (currentTimeMillis() -before);
        log.info("Time for iteration over " + relCount + " relations = " + readRelationshipMillis + " millis");

        nodeMapping = new int[getNodeCount()];
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

    public int getOrCreateAlgoNodeId(long node) {
        if (nodeMap.containsKey(node)) {
            return nodeMap.get(node);
        }
        synchronized (nodeMap) {
            if (nodeMap.containsKey(node)) {
                return nodeMap.get(node);
            }
            int assignedId = maxAlgoNodeId.getAndIncrement();
            nodeMap.put(node, assignedId);
            return assignedId;
        }
    }

    @Override
    public int getNodeCount() {
        return maxAlgoNodeId.intValue();
    }

    // TODO Create buckets instead of copying data.
    // Not doing it right now because of the complications of the interface.
    private static int [] growArray(int[] array, float growFactor) {
        int currentSize = array.length;
        int newArray[] = new int[(int)(currentSize * growFactor)];
        System.arraycopy(array, 0, newArray, 0, currentSize);
        return newArray;
    }

    private int loadRelationships(String relCypher, boolean weighted, int defaultWeight) {
        RelationshipLoader loader = new RelationshipLoader(weighted, defaultWeight, this);
        db.execute(COMPILED_RUNTIME + relCypher).accept(loader);
        loader.calculateChunkIndices();

        int totalRelationships = loader.totalRelationships;
        Chunks relationshipTargetChunks = new Chunks(totalRelationships);
        Chunks relationshipWeightChunks =  weighted ? null : new Chunks(totalRelationships).withDefault(defaultWeight);
        int[] offsetTracker = loader.sourceChunkStartingIndex.mergeChunks();
        loader.transformRelData(relationshipTargetChunks,relationshipWeightChunks, offsetTracker);
        this.sourceDegreeData = loader.sourceDegreeData.mergeAllChunks(); // todo allow for only degree's of source nodes
        this.sourceChunkStartingIndex = loader.sourceChunkStartingIndex.mergeChunks();
        this.relationshipTarget = relationshipTargetChunks.mergeChunks();
        this.relationshipWeight = relationshipWeightChunks == null ? null : relationshipWeightChunks.mergeAllChunks();
        return totalRelationships;
    }
    private int loadRelationshipsBatch(String relCypher, boolean weighted, int defaultWeight, int batchSize) {
        // we don't know how much data we're loading
        // so we parallelize for all CPUs for one round and then see if the last one didn't load any data
        int threads = Pools.getNoThreadsInDefaultPool();
        int batch = 0;
        int totalRelationships = 0;
        int nonLoaded = 0;
        List<RelationshipLoader> loaders = new ArrayList<>();
        do {
            List<Future<RelationshipLoader>> futures = new ArrayList<>();
            for (int i = 0; i < threads; i++) {
                Map<String, Object> params = map("skip", batch * batchSize, "limit", batchSize);
                Future<RelationshipLoader> future = Util.inFuture(() -> {
                    RelationshipLoader loader = new RelationshipLoader(weighted, defaultWeight, (AlgoIdGenerator) this);
                    db.execute(COMPILED_RUNTIME + relCypher, params).accept(loader);
                    return loader;
                });
                futures.add(future);
                batch++;
            }
            for (Future<RelationshipLoader> future : futures) {
                RelationshipLoader loader = get(future);
                totalRelationships += loader.totalRelationships;
                if (loader.totalRelationships == 0) nonLoaded++;
                else loaders.add(loader);
            }
        } while (nonLoaded == 0);

        int nodeCount = getNodeCount();


        Chunks relationshipTargetChunks = new Chunks(totalRelationships);
        Chunks relationshipWeightChunks = weighted ? new Chunks(totalRelationships).withDefault(defaultWeight) : null;

        Chunks sourceDegrees = new Chunks(nodeCount);
        for (RelationshipLoader loader : loaders) {
            sourceDegrees.add(loader.sourceDegreeData);
        }
        Chunks offsets = sourceDegrees.clone();
        offsets.sumUp();

        int[] offsetTracker = offsets.mergeChunks();
        for (RelationshipLoader loader : loaders) {
            loader.transformRelData(relationshipTargetChunks, relationshipWeightChunks, offsetTracker);
        }
        this.sourceDegreeData = sourceDegrees.mergeChunks();
        this.sourceChunkStartingIndex = offsets.mergeChunks();
        this.relationshipTarget = relationshipTargetChunks.mergeChunks();
        this.relationshipWeight = relationshipWeightChunks == null ? null : relationshipWeightChunks.mergeAllChunks();

        return totalRelationships;
    }

    private RelationshipLoader get(Future<RelationshipLoader> future)  {
        try {
            return future.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException("Error loading relationships",e);
        }
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

    private static class RelationshipLoader implements Result.ResultVisitor<RuntimeException> {
        private final AlgoIdGenerator algoIdGenerator;
        private final boolean weighted;
        private final int defaultWeight;
        int totalRelationships = 0;

        int sourceIndex = 0;
        int targetIndex = 0;
        long lastSource = -1, lastTarget = -1;

        int relDataIdx = 0;
        private Chunks relData;

        private Chunks relationshipWeight;
        private Chunks sourceDegreeData;
        private Chunks sourceChunkStartingIndex;
        private PrimitiveLongIntMap weights;

        public RelationshipLoader(boolean weighted, int defaultWeight, AlgoIdGenerator algoIdGenerator) {
            this.weighted = weighted;
            this.defaultWeight = defaultWeight;
            int nodeCount = algoIdGenerator.getNodeCount();

            relData = new Chunks(nodeCount);
            this.algoIdGenerator = algoIdGenerator;

            sourceDegreeData = new Chunks(nodeCount).withDefault(0);

            if (weighted) {
                weights = Primitive.longIntMap(INITIAL_ARRAY_SIZE);
            }
        }

        @Override
        public boolean visit(Result.ResultRow res) throws RuntimeException {
            totalRelationships++;

            long source = ((Number) res.get("source")).longValue();
            if (lastSource != source) {
                sourceIndex = algoIdGenerator.getOrCreateAlgoNodeId(source);
                relData.set(relDataIdx++,-sourceIndex - 1);
                lastSource = source;
            }

            long target = ((Number) res.get("target")).longValue();
            if (lastTarget != target) {
                targetIndex = algoIdGenerator.getOrCreateAlgoNodeId(target);
                lastTarget = target;
            }
            if (weighted) {
                Number weightValue = res.getNumber("weight");
                if (weightValue != null) {
                    int weight = weightValue.intValue();
                    if (weight != defaultWeight) {
                        long key = ((long) sourceIndex) << 32 | targetIndex;
                        weights.put(key, weight);
                    }
                }
            }
            relData.set(relDataIdx++,targetIndex);
            sourceDegreeData.increment(sourceIndex);
            return true;
        }

        private void transformRelData(Chunks relationshipTargetChunks, Chunks relationshipWeightChunks, int[] offsetTracker) {
            if (weighted) {
                transformRelationshipDataToOffsetStorage(relData, relDataIdx, weights, relationshipTargetChunks, relationshipWeightChunks, offsetTracker);
                weights.clear();
            } else {
                transformRelationshipDataToOffsetStorage(relData, relDataIdx, relationshipTargetChunks, offsetTracker);
            }
            relData.clear();
        }

        private void transformRelationshipDataToOffsetStorage(Chunks relData, int relDataIdx, PrimitiveLongIntMap weights, Chunks relationshipTarget, Chunks relationshipWeight, int[] offsetTracker) {
            int sourceIndex = 0;
            //            if (defaultWeight!=0) Arrays.fill(relationshipWeight,defaultWeight);
            for (int i=0;i<relDataIdx;i++) {
                int id = relData.get(i);
                if (id < 0) {
                    sourceIndex = -id-1;
                } else {
                    long key = ((long) sourceIndex) << 32 | id;
                    if (weights.containsKey(key)) {
                       relationshipWeight.set(offsetTracker[sourceIndex],weights.get(key));
                    }
                    relationshipTarget.set(offsetTracker[sourceIndex],id);
                    offsetTracker[sourceIndex]++;
                }
            }
        }

        private void calculateChunkIndices() {
            int nodes = algoIdGenerator.getNodeCount();
            sourceChunkStartingIndex = new Chunks(nodes);
            int currentOffset = 0;
            for (int i = 0; i < nodes; i++) {
                sourceChunkStartingIndex.set(i,currentOffset);
                currentOffset += sourceDegreeData.get(i);
            }
        }

        private void transformRelationshipDataToOffsetStorage(Chunks relData, int relDataIdx, Chunks relationshipTarget, int[] offsetTracker) {
            int sourceIndex = 0;
            for (int i=0;i<relDataIdx;i++) {
                int id = relData.get(i);
                if (id < 0) {
                    sourceIndex = -id-1;
                } else {
                    relationshipTarget.set(offsetTracker[sourceIndex]++,id);
                }
            }
        }
    }
}
