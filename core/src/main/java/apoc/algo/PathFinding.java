package apoc.algo;

import apoc.result.PathResult;
import apoc.result.WeightedPathResult;
import apoc.util.Util;
import org.neo4j.graphalgo.*;
import org.neo4j.graphdb.*;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Collections;
import java.util.Map;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import static apoc.algo.PathFindingUtils.buildPathExpander;

public class PathFinding {

    @Context
    public GraphDatabaseService db;

    @Context
    public Transaction tx;

    @Procedure
    @Description("apoc.algo.aStar(startNode, endNode, 'KNOWS|<WORKS_WITH|IS_MANAGER_OF>', 'distance','lat','lon') " +
            "YIELD path, weight - run A* with relationship property name as cost function")
    public Stream<WeightedPathResult> aStar(
            @Name("startNode") Node startNode,
            @Name("endNode") Node endNode,
            @Name("relationshipTypesAndDirections") String relTypesAndDirs,
            @Name("weightPropertyName") String weightPropertyName,
            @Name("latPropertyName") String latPropertyName,
            @Name("lonPropertyName") String lonPropertyName) {

        PathFinder<WeightedPath> algo = GraphAlgoFactory.aStar(
                new BasicEvaluationContext(tx, db),
                buildPathExpander(relTypesAndDirs),
                CommonEvaluators.doubleCostEvaluator(weightPropertyName),
                CommonEvaluators.geoEstimateEvaluator(latPropertyName, lonPropertyName));
        return WeightedPathResult.streamWeightedPathResult(startNode, endNode, algo);
    }

    @Procedure
    @Description("apoc.algo.aStarConfig(startNode, endNode, 'KNOWS|<WORKS_WITH|IS_MANAGER_OF>', {weight:'dist',default:10," +
            "x:'lon',y:'lat', pointPropName:'point'}) YIELD path, weight - run A* with relationship property name as cost function")
    public Stream<WeightedPathResult> aStarConfig(
            @Name("startNode") Node startNode,
            @Name("endNode") Node endNode,
            @Name("relationshipTypesAndDirections") String relTypesAndDirs,
            @Name("config") Map<String, Object> config) {

        config = config == null ? Collections.emptyMap() : config;
        String relationshipCostPropertyKey = config.getOrDefault("weight", "distance").toString();
        double defaultCost = ((Number) config.getOrDefault("default", Double.MAX_VALUE)).doubleValue();
        String pointPropertyName = (String) config.get("pointPropName");
        final EstimateEvaluator<Double> estimateEvaluator;
        if (pointPropertyName != null) {
            estimateEvaluator = new PathFindingUtils.GeoEstimateEvaluatorPointCustom(pointPropertyName);
        } else {
            String latPropertyName = config.getOrDefault("y", "latitude").toString();
            String lonPropertyName = config.getOrDefault("x", "longitude").toString();
            estimateEvaluator = CommonEvaluators.geoEstimateEvaluator(latPropertyName, lonPropertyName);
        }
        PathFinder<WeightedPath> algo = GraphAlgoFactory.aStar(
                new BasicEvaluationContext(tx, db),
                buildPathExpander(relTypesAndDirs),
                CommonEvaluators.doubleCostEvaluator(relationshipCostPropertyKey, defaultCost),
                estimateEvaluator);
        return WeightedPathResult.streamWeightedPathResult(startNode, endNode, algo);
    }

    @Procedure
    @Description("apoc.algo.dijkstra(startNode, endNode, 'KNOWS|<WORKS_WITH|IS_MANAGER_OF>', 'distance', defaultValue, numberOfWantedResults) YIELD path," +
            " weight - run dijkstra with relationship property name as cost function")
    public Stream<WeightedPathResult> dijkstra(
            @Name("startNode") Node startNode,
            @Name("endNode") Node endNode,
            @Name("relationshipTypesAndDirections") String relTypesAndDirs,
            @Name("weightPropertyName") String weightPropertyName,
            @Name(value = "defaultWeight", defaultValue = "NaN") double defaultWeight,
            @Name(value = "numberOfWantedPaths", defaultValue = "1") long numberOfWantedPaths) {

        PathFinder<WeightedPath> algo = GraphAlgoFactory.dijkstra(
                buildPathExpander(relTypesAndDirs),
                (relationship, direction) -> Util.toDouble(relationship.getProperty(weightPropertyName, defaultWeight)),
                (int)numberOfWantedPaths
        );
        return WeightedPathResult.streamWeightedPathResult(startNode, endNode, algo);
    }

    @Procedure
    @Description("apoc.algo.allSimplePaths(startNode, endNode, 'KNOWS|<WORKS_WITH|IS_MANAGER_OF>', 5) YIELD path, " +
            "weight - run allSimplePaths with relationships given and maxNodes")
    public Stream<PathResult> allSimplePaths(
            @Name("startNode") Node startNode,
            @Name("endNode") Node endNode,
            @Name("relationshipTypesAndDirections") String relTypesAndDirs,
            @Name("maxNodes") long maxNodes) {

        PathFinder<Path> algo = GraphAlgoFactory.allSimplePaths(
                new BasicEvaluationContext(tx, db),
                buildPathExpander(relTypesAndDirs),
                (int) maxNodes
        );
        Iterable<Path> allPaths = algo.findAllPaths(startNode, endNode);
        return StreamSupport.stream(allPaths.spliterator(), false)
                .map(PathResult::new);
    }

}
