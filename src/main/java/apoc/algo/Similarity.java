package apoc.algo;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

import java.util.List;


public class Similarity {
    @Context
    public GraphDatabaseService db;

    @UserFunction
    @Description("apoc.algo.cosineSimilarity([vector1], [vector2]) " +
            "given two collection vectors, calculate cosine similarity")
    public double cosineSimilarity(@Name("vector1") List<Number> vector1, @Name("vector2") List<Number> vector2) {
        if (vector1.size() != vector2.size() || vector1.size() == 0) {
            throw new RuntimeException("Vectors must be non-empty and of the same size");
        }

        double dotProduct = 0d;
        double xLength = 0d;
        double yLength = 0d;
        for (int i = 0; i < vector1.size(); i++) {
            double weight1 = vector1.get(i).doubleValue();
            double weight2 = vector2.get(i).doubleValue();

            dotProduct += weight1 * weight2;
            xLength += weight1 * weight1;
            yLength += weight2 * weight2;
        }

        xLength = Math.sqrt(xLength);
        yLength = Math.sqrt(yLength);

        return dotProduct / (xLength * yLength);
    }

    @UserFunction
    @Description("apoc.algo.euclideanDistance([vector1], [vector2]) " +
            "given two collection vectors, calculate the euclidean distance (square root of the sum of the squared differences)")
    public double euclideanDistance(@Name("vector1") List<Number> vector1, @Name("vector2") List<Number> vector2) {
        if (vector1.size() != vector2.size() || vector1.size() == 0) {
            throw new RuntimeException("Vectors must be non-empty and of the same size");
        }

        double distance = 0.0;
        for (int i = 0; i < vector1.size(); i++) {
            double sqOfDiff = vector1.get(i).doubleValue() - vector2.get(i).doubleValue();
            sqOfDiff *= sqOfDiff;
            distance += sqOfDiff;
        }
        distance = Math.sqrt(distance);

        return distance;
    }

    @UserFunction
    @Description("apoc.algo.euclideanSimilarity([vector1], [vector2]) " +
            "given two collection vectors, calculate similarity based on euclidean distance")
    public double euclideanSimilarity(@Name("vector1") List<Number> vector1, @Name("vector2") List<Number> vector2) {
        return 1.0d / (1 + euclideanDistance(vector1, vector2));
    }
}
