package apoc.algo;

import apoc.Extended;
import apoc.result.WeightedPathResult;
import org.neo4j.graphalgo.BasicEvaluationContext;
import org.neo4j.graphalgo.CommonEvaluators;
import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphalgo.WeightedPath;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.stream.Stream;
import static apoc.algo.PathFindingUtils.buildPathExpander;

@Extended
public class PathFindingExtended {

    @Context
    public GraphDatabaseService db;

    @Context
    public Transaction tx;

    @Procedure
    @Description("apoc.algo.aStarWithPoint(startNode, endNode, 'relTypesAndDirs', 'distance','pointProp') - " +
            "equivalent to apoc.algo.aStar but accept a Point type as a pointProperty instead of Number types as latitude and longitude properties")
    public Stream<WeightedPathResult> aStarWithPoint(
            @Name("startNode") Node startNode,
            @Name("endNode") Node endNode,
            @Name("relationshipTypesAndDirections") String relTypesAndDirs,
            @Name("weightPropertyName") String weightPropertyName,
            @Name("pointPropertyName") String pointPropertyName) {

        PathFinder<WeightedPath> algo = GraphAlgoFactory.aStar(
                new BasicEvaluationContext(tx, db),
                buildPathExpander(relTypesAndDirs),
                CommonEvaluators.doubleCostEvaluator(weightPropertyName),
                new PathFindingUtils.GeoEstimateEvaluatorPointCustom(pointPropertyName));
        return WeightedPathResult.streamWeightedPathResult(startNode, endNode, algo);
    }
    
}
