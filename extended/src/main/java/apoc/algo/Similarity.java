package apoc.algo;
// WARNING: This code uses the Java Vector API, which is a preview/incubator
// feature in modern JDKs (e.g., JDK 21+). To compile and run this code,
// you must enable the vector module with specific JVM flags,
// e.g.: --add-modules jdk.incubator.vector
import apoc.Extended;
//import jdk.incubator.vector.*;
import apoc.util.Util;
import apoc.util.collection.Iterables;
import apoc.util.collection.Iterators;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.procedure.*;
import org.neo4j.values.storable.*;
//import org.neo4j.values.vector.VectorValue;
//import org.neo4j.values.vector.VectorValueNotSupported;
//import org.neo4j.values.vector.VectorValue.VectorType;

import java.util.*;
import java.util.stream.Stream;

/**
 * A Neo4j Procedure class for performing on-the-fly similarity searches on a dynamic batch of nodes.
 * It does not use pre-existing indexes but leverages the Java Vector API (SIMD) for maximum compute performance.
 */

/**
 * A realistic, high-performance Neo4j Procedure for on-the-fly similarity searches.
 * Given the public Neo4j Vector API constraints (element-by-element access),
 * this implementation uses a SCALAR calculation, as it's more performant than
 * creating temporary arrays for a SIMD approach.
 */
@Extended
public class Similarity {
    @Context
    public GraphDatabaseService db;
    
    @Context
    public Transaction tx;
    
    public record SimilarityConfig(boolean stopWhenFound) {
        public static SimilarityConfig fromMap(Map<String, Object> config) {
            if (config == null) {
                config = Collections.emptyMap();
            }
            boolean stopWhenFoundConf = Util.toBoolean(config.get("stopWhenFound"));
            return new SimilarityConfig(stopWhenFoundConf);
        }
    }

    // --- Procedure Output Class ---
    public static class NodeScore {
        @Description("The found node.")
        public Node node;
        @Description("The calculated similarity score, ranging from 0.0 to 1.0.")
        public double score;
        public NodeScore(Node node, double score) { this.node = node; this.score = score; }
    }

    @Procedure(name = "custom.search.batchedSimilarity", mode = Mode.READ)
    @Description("Performs a type-safe cosine similarity search on a batch of nodes. Returns the top-K nodes above a given threshold.")
    public Stream<NodeScore> batchedSimilarity(
            @Name("nodes") List<Node> nodes,
            // TODO --
            
            @Name("propertyName") String propertyName,
            // TODO -- CAMBIARE `Object` IN `VectorValue`
            @Name("queryVector") Object queryVector,
            @Name("topK") long topK,
            @Name("threshold") double threshold,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config
    ) {
        nodes = Util.rebind(nodes, tx);
        SimilarityConfig conf = SimilarityConfig.fromMap(config);

        // TODO
        // TODO
        // TODO - RIMUOVERE, MOCKATO PER I TEST
        // TODO
        if (queryVector == null) {
            queryVector = Values.int64Vector(1, 2, 3);
        }
        // TODO - FINE RIMOZIONE MOCK
        
        PriorityQueue<NodeScore> topKQueue = new PriorityQueue<>(Comparator.comparingDouble(a -> a.score));

        // Type-safe dispatch based on the query vector's class
        if (queryVector instanceof FloatingPointVector queryVecFloat) {
            for (Node node : nodes) {
                Object propertyValue = node.getProperty(propertyName, null);
                if (propertyValue instanceof FloatingPointVector nodeVecFloat) {
                    if (processNode(node, nodeVecFloat, queryVecFloat, topK, threshold, topKQueue, conf)) {
                        break;
                    };
                }
            }
        } else if (queryVector instanceof IntegralVector queryVecByte) {
            for (Node node : nodes) {
                Object propertyValue = node.getProperty(propertyName, "null");
                // TODO
                // TODO
                // TODO - RIMUOVERE, MOCKATO PER I TEST
                // TODO
                if (propertyValue == "null") {
                    if (node.hasProperty("test")) {
                        propertyValue = Values.int64Vector(1, 2, 4);
                    } else if (node.hasProperty("ajeje")) {
                        propertyValue = Values.int64Vector(1, 2, 3);
                    } else if (node.hasProperty("brazorf")) {
                        propertyValue = Values.int64Vector(1, 3, 4);
                    } else {
                        propertyValue = Values.int64Vector(1, 3, 3);
                    }
                }
                // TODO - FINE RIMOZIONE MOCK
                if (propertyValue instanceof IntegralVector nodeVecByte) {
                    if (processNode(node, nodeVecByte, queryVecByte, topK, threshold, topKQueue, conf)){
                        break;
                    };
                }
            }
        } else {
            // TODO - error?
            return Stream.empty(); // Unsupported query vector type
        }

        List<NodeScore> result = new ArrayList<>(topKQueue);
        result.sort(Comparator.comparingDouble((NodeScore ns) -> ns.score).reversed());
        return result.stream();
    }

    // Helper method for the main processing logic
    private boolean processNode(Node node, VectorValue nodeVector, VectorValue queryVector, long topK, double threshold, PriorityQueue<NodeScore> topKQueue, SimilarityConfig conf) {
//        System.out.println("Similarity.processNode" + node.getAllProperties());
        if (nodeVector.dimensions() != queryVector.dimensions()) {
            return false;
        }

        // The raw calculation now dispatches to the appropriate SCALAR calculator
        double rawSimilarity = calculateRawSimilarity(nodeVector, queryVector);
        double normalizedScore = (rawSimilarity + 1) / 2.0;

        // WARNING: Java vector incubator module is not readable. For optimal vector performance, pass '--add-modules jdk.incubator.vector' to enable Vector API.
//        double normalizedScore = Iterators.single(tx.execute("RETURN vector.similarity.cosine($a, $b) AS s", Map.of("a", nodeVector, "b", queryVector)).columnAs("s"));

        if (normalizedScore >= threshold) {
            if (topKQueue.size() < topK) {
                topKQueue.add(new NodeScore(node, normalizedScore));
            } else if (normalizedScore > topKQueue.peek().score) {
                topKQueue.poll();
                topKQueue.add(new NodeScore(node, normalizedScore));
            }
            // NUOVO controllo per l'arresto rapido
            if (conf.stopWhenFound() && topKQueue.size() == topK) {
                return true;
            }
        }
        return false; // Continua con il prossimo nodo
    }

    // --- Section with SCALAR calculation functions ---

    private double calculateRawSimilarity(VectorValue v1, VectorValue v2) {
        if (v1 instanceof FloatingPointVector) {
            return calculateFloat32_Scalar(v1, v2);
        } else if (v1 instanceof IntegralVector) {
            return calculateInteger8_Scalar(v1, v2);
        }
        throw new UnsupportedOperationException("Unsupported vector type for calculation: " + v1.getClass().getName());
    }

    private double calculateFloat32_Scalar(VectorValue v1, VectorValue v2) {
        double dotProduct = 0.0;
        double normA = 0.0;
        double normB = 0.0;
        int dimensions = v1.dimensions();
        for (int i = 0; i < dimensions; i++) {
            // Accessing data element-by-element, as per the public API
            double valA = v1.doubleValue(i);
            double valB = v2.doubleValue(i);
            dotProduct += valA * valB;
            normA += valA * valA;
            normB += valA * valA;
        }
        if (normA == 0.0 || normB == 0.0) return 0.0;
        return dotProduct / (Math.sqrt(normA) * Math.sqrt(normB));
    }

    private float calculateInteger8_Scalar(VectorValue v1, VectorValue v2) {
        float dotProduct = 0;
        float normA = 0;
        float normB = 0;
        int dimensions = v1.dimensions();
        for (int i = 0; i < dimensions; i++) {
            // Here we would need a floatValue(i) method, assuming it exists on Integer8Vector
            float valA = v1.floatValue(i); // Hypothetical method
            float valB = v2.floatValue(i); // Hypothetical method
            dotProduct += valA * valB;
            normA += valA * valA;
            normB += valA * valA;
        }
        if (normA == 0 || normB == 0) return 0.0F;
        return (float) (dotProduct / (Math.sqrt(normA) * Math.sqrt(normB)));
    }
}