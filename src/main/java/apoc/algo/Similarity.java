package apoc.algo;

import apoc.util.Util;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Relationship;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

import java.util.ArrayList;
import java.util.List;


public class Similarity {
    @Context
    public GraphDatabaseService db;

    @UserFunction
    @Description("apoc.algo.cosineSimilarity([[rel1A, rel1B], [rel2A, rel2B], …], weightPropertyName, defaultWeight: 0) " +
            "given a collection of collected pairs of relationships, the weight property of the relationships, " +
            "and a default weight (if relationship or property is null), calculate cosine similarity")
    public Double cosineSimilarity(
            @Name("relPairs") List<List<Relationship>> relPairs,
            @Name("weightPropertyName") String weightPropertyName,
            @Name(value = "defaultWeight", defaultValue = "0.0") Double defaultWeight) {

        List<Double> weight1 = new ArrayList<>();
        List<Double> weight2 = new ArrayList<>();

        relPairs.stream().forEach(relationships -> {
            Relationship rel = relationships.get(0);
            weight1.add(rel == null ? defaultWeight : Util.doubleValue(rel, weightPropertyName, defaultWeight));

            rel = relationships.get(1);
            weight2.add(rel == null ? defaultWeight : Util.doubleValue(rel, weightPropertyName, defaultWeight));
        });

        Double dotProduct = 0d;
        for (int i = 0, sum = 0; i < weight1.size(); i++) {
            dotProduct += weight1.get(i) * weight2.get(i);
        }

        Double xLength = Math.sqrt(weight1.stream().reduce(0.0d, (runningTotal, nextWeight) -> runningTotal + Math.pow(nextWeight, 2)));
        Double yLength = Math.sqrt(weight2.stream().reduce(0.0d, (runningTotal, nextWeight) -> runningTotal + Math.pow(nextWeight, 2)));

        Double similarity = dotProduct / (xLength * yLength);

        return similarity;
    }
}
