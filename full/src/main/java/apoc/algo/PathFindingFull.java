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

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static apoc.algo.PathFinding.buildPathExpander;

@Extended
public class PathFindingFull {

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
                new PathFinding.GeoEstimateEvaluatorPointCustom(pointPropertyName));
        return WeightedPathResult.streamWeightedPathResult(startNode, endNode, algo);
    }

    @Procedure
    @Description("apoc.algo.travellingSalesman(nodes,  $config) - resolve travelling salesman problem via simulated annealing algo")
    public Stream<TravellingSalesman.Result> travellingSalesman(@Name("startNode") List<Node> nodes, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        if (nodes.isEmpty()) {
            throw new RuntimeException("The nodes parameter must have at least 3 nodes");
        }
        TravellingSalesman.Config conf = new TravellingSalesman.Config(config);
        return Stream.of(TravellingSalesman.Algo.simulateAnnealing(nodes, conf));
    }
    
}
