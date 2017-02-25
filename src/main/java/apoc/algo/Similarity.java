package apoc.algo;

import apoc.util.Util;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Relationship;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

import java.util.List;


public class Similarity {
    @Context
    public GraphDatabaseService db;

    @UserFunction
    @Description("apoc.algo.cosineSimilarity([[rel1A, rel1B], [rel2A, rel2B], …], weightPropertyName, defaultWeight: 0.0) " +
            "given a collection of collected pairs of relationships, the weight property of the relationships, " +
            "and a default weight (if relationship or property is null), calculate cosine similarity")
    public double cosineSimilarity(
            @Name("relPairs") List<List<Relationship>> relPairs,
            @Name("weightPropertyName") String weightPropertyName,
            @Name(value = "defaultWeight", defaultValue = "0.0d") double defaultWeight) {

        double dotProduct = 0d;
        double xLength = 0d;
        double yLength = 0d;
        for (int i = 0; i < relPairs.size(); i++) {
            Relationship rel1 = relPairs.get(i).get(0);
            double weight1 = (rel1 == null ? defaultWeight : Util.doubleValue(rel1, weightPropertyName, defaultWeight));
            Relationship rel2 = relPairs.get(i).get(1);
            double weight2 = (rel2 == null ? defaultWeight : Util.doubleValue(rel2, weightPropertyName, defaultWeight));

            dotProduct += weight1 * weight2;
            xLength += weight1 * weight1;
            yLength += weight2 * weight2;
        }

        xLength = Math.sqrt(xLength);
        yLength = Math.sqrt(yLength);

        return dotProduct / (xLength * yLength);
    }
}
