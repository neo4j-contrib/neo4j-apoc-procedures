//package apoc.algo;
//// WARNING: This code uses the Java Vector API, which is a preview/incubator
//// feature in modern JDKs (e.g., JDK 21+). To compile and run this code,
//// you must enable the vector module with specific JVM flags,
//// e.g.: --add-modules jdk.incubator.vector
//
//import apoc.Extended;
//import jdk.incubator.vector.*;
//import org.neo4j.graphdb.Node;
//import org.neo4j.procedure.Description;
//import org.neo4j.procedure.Mode;
//import org.neo4j.procedure.Name;
//import org.neo4j.procedure.Procedure;
//import org.neo4j.values.storable.VectorValue;
//
//import java.util.ArrayList;
//import java.util.Comparator;
//import java.util.List;
//import java.util.PriorityQueue;
//import java.util.stream.Stream;
//
///**
// * A Neo4j Procedure class for performing on-the-fly similarity searches on a dynamic batch of nodes.
// * It does not use pre-existing indexes but leverages the Java Vector API (SIMD) for maximum compute performance.
// */
//
//@Extended
//public class SimilarityToDelete {
//
//    // --- Procedure Output Class ---
//    public static class NodeScore {
//        @Description("The found node.")
//        public Node node;
//        @Description("The calculated similarity score, ranging from 0.0 to 1.0.")
//        public double score;
//
//        public NodeScore(Node node, double score) {
//            this.node = node;
//            this.score = score;
//        }
//    }
//
//    // --- SIMD Vector Species ---
//    // Select the preferred SIMD vector "shape" (species) for floats from the CPU.
//    private static final VectorSpecies<Float> FLOAT_SPECIES = FloatVector.SPECIES_PREFERRED;
//    // Select the preferred SIMD vector "shape" (species) for bytes from the CPU.
//    private static final VectorSpecies<Byte> BYTE_SPECIES = ByteVector.SPECIES_PREFERRED;
//    // The corresponding integer species when widening bytes.
//    private static final VectorSpecies<Integer> INT_SPECIES_FOR_BYTE_WIDENING = ByteVector.SPECIES_PREFERRED.widenedspecies(int.class);
//
//    @Procedure(name = "custom.search.batchedSimilarity", mode = Mode.READ)
//    @Description("Performs a cosine similarity search on a batch of nodes using SIMD. Returns the top-K nodes above a given threshold.")
//    public Stream<NodeScore> batchedSimilarity(
//            @Name("nodes") List<Node> nodes,
//            @Name("propertyName") String propertyName,
//            @Name("queryVector") VectorValue queryVector,
//            @Name("topK") long topK,
//            @Name("threshold") double threshold
//    ) {
//        // Use a PriorityQueue to efficiently maintain the top-K elements (as a min-heap).
//        PriorityQueue<NodeScore> topKQueue = new PriorityQueue<>(Comparator.comparingDouble(a -> a.score));
//
//        for (Node node : nodes) {
//            // Requirement: Kernel Neo4j API for property access
//            Object propertyValue = node.getProperty(propertyName, null);
//            if (!(propertyValue instanceof VectorValue nodeVector)) {
//                // Skip the node if it doesn't have the property or it's not a vector.
//                continue;
//            }
//
//            // Compatibility checks
//            if (nodeVector.size() != queryVector.size() || nodeVector.valueType().realType() != queryVector.valueType().realType()) {
//                continue;
//            }
//
//            // Calculate the raw similarity score [-1, 1] using the appropriate SIMD function
//            double rawSimilarity = calculateRawSimilarity(nodeVector, queryVector);
//
//            // Normalize the score to the [0, 1] range
//            double normalizedScore = (rawSimilarity + 1) / 2.0;
//
//            // Requirement: Early filtering and stop... (threshold)
//            if (normalizedScore < threshold) {
//                // Immediately discard nodes below the threshold.
//                continue;
//            }
//
//            // Requirement: Early filtering and stop... (top-k)
//            if (topKQueue.size() < topK) {
//                topKQueue.add(new NodeScore(node, normalizedScore));
//            } else if (normalizedScore > topKQueue.peek().score) {
//                topKQueue.poll(); // Remove the worst element (lowest score).
//                topKQueue.add(new NodeScore(node, normalizedScore)); // Add the new best one.
//            }
//        }
//
//        // Convert the queue to a sorted list and return it as a stream.
//        List<NodeScore> result = new ArrayList<>(topKQueue);
//        result.sort(Comparator.comparingDouble((NodeScore ns) -> ns.score).reversed());
//        return result.stream();
//    }
//
//    // --- Section with SIMD calculation functions ---
//
//    private double calculateRawSimilarity(VectorValue v1, VectorValue v2) {
//        switch (v1.valueType().realType()) {
//            case FLOAT32:
//                return calculateFloat32_SIMD(v1.floatArray(), v2.floatArray());
//            case INTEGER8:
//                return calculateInteger8_SIMD(v1.byteArray(), v2.byteArray());
//            default:
//                throw new VectorValueNotSupported("Vector type not supported for cosine similarity: " + v1.valueType().realType());
//        }
//    }
//
//    private double calculateFloat32_SIMD(float[] v1, float[] v2) {
//        FloatVector dotProductVec = FloatVector.zero(FLOAT_SPECIES);
//        FloatVector normAVec = FloatVector.zero(FLOAT_SPECIES);
//        FloatVector normBVec = FloatVector.zero(FLOAT_SPECIES);
//        int loopBound = FLOAT_SPECIES.loopBound(v1.length);
//        for (int i = 0; i < loopBound; i += FLOAT_SPECIES.length()) {
//            FloatVector va = FloatVector.fromArray(FLOAT_SPECIES, v1, i);
//            FloatVector vb = FloatVector.fromArray(FLOAT_SPECIES, v2, i);
//            dotProductVec = va.fma(vb, dotProductVec);
//            normAVec = va.fma(va, normAVec);
//            normBVec = vb.fma(vb, normBVec);
//        }
//        double dotProduct = dotProductVec.reduceLanes(VectorOperators.ADD);
//        double normA = normAVec.reduceLanes(VectorOperators.ADD);
//        double normB = normBVec.reduceLanes(VectorOperators.ADD);
//        // Scalar loop for the "tail"
//        for (int i = loopBound; i < v1.length; i++) {
//            dotProduct += (double) v1[i] * v2[i];
//            normA += (double) v1[i] * v1[i];
//            normB += (double) v2[i] * v2[i];
//        }
//        if (normA == 0.0 || normB == 0.0) return 0.0;
//        return dotProduct / (Math.sqrt(normA) * Math.sqrt(normB));
//    }
//
//    private double calculateInteger8_SIMD(byte[] v1, byte[] v2) {
//        // Accumulators MUST be of a wider type (Int/Long) to prevent overflow.
//        IntVector dotProductVec = IntVector.zero(INT_SPECIES_FOR_BYTE_WIDENING);
//        IntVector normAVec = IntVector.zero(INT_SPECIES_FOR_BYTE_WIDENING);
//        IntVector normBVec = IntVector.zero(INT_SPECIES_FOR_BYTE_WIDENING);
//        int loopBound = BYTE_SPECIES.loopBound(v1.length);
//        for (int i = 0; i < loopBound; i += BYTE_SPECIES.length()) {
//            ByteVector ba = ByteVector.fromArray(BYTE_SPECIES, v1, i);
//            ByteVector bb = ByteVector.fromArray(BYTE_SPECIES, v2, i);
//            // "Widen" byte vectors to int vectors to perform calculations safely, avoiding overflow.
//            IntVector ia = ba.widen(VectorOperators.UNSIGNED_BYTE);
//            IntVector ib = bb.widen(VectorOperators.UNSIGNED_BYTE);
//            // Perform operations on the int vectors.
//            dotProductVec = ia.mul(ib).add(dotProductVec);
//            normAVec = ia.mul(ia).add(normAVec);
//            normBVec = ib.mul(ib).add(normBVec);
//        }
//        // Reduce to 'long' to be 100% safe from overflow on the final sum.
//        long dotProduct = dotProductVec.reduceLanesToLong(VectorOperators.ADD);
//        long normA = normAVec.reduceLanesToLong(VectorOperators.ADD);
//        long normB = normBVec.reduceLanesToLong(VectorOperators.ADD);
//        // Scalar loop for the "tail"
//        for (int i = loopBound; i < v1.length; i++) {
//            dotProduct += (long) v1[i] * v2[i];
//            normA += (long) v1[i] * v1[i];
//            normB += (long) v2[i] * v2[i];
//        }
//        if (normA == 0 || normB == 0) return 0.0;
//        return (double) dotProduct / (Math.sqrt((double) normA) * Math.sqrt((double) normB));
//    }
//}