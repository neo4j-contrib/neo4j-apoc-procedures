package apoc.algo;

import apoc.Description;
import apoc.path.RelationshipTypeAndDirections;
import apoc.result.PathResult;
import org.neo4j.graphalgo.*;
import org.neo4j.graphdb.*;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Collections;
import java.util.Map;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class Algo {

    @Procedure
    @Description("apoc.algo.aStar(startNode, endNode, 'KNOWS|<WORKS_WITH|IS_MANAGER_OF>', 'distance','lat','lon') YIELD path, weight - run A* with relationship property name as cost function")
    public Stream<WeightedPathResult> aStar(
            @Name("startNode") Node startNode,
            @Name("endNode") Node endNode,
            @Name("relationshipTypesAndDirections") String relTypesAndDirs,
            @Name("weightPropertyName") String weightPropertyName,
            @Name("latPropertyName") String latPropertyName,
            @Name("lonPropertyName") String lonPropertyName) {

        PathFinder<WeightedPath> algo = GraphAlgoFactory.aStar(
                buildPathExpander(relTypesAndDirs),
                CommonEvaluators.doubleCostEvaluator(weightPropertyName),
                CommonEvaluators.geoEstimateEvaluator(latPropertyName,lonPropertyName));
        return streamWeightedPathResult(startNode, endNode, algo);
    }

    @Procedure
    @Description("apoc.algo.aStar(startNode, endNode, 'KNOWS|<WORKS_WITH|IS_MANAGER_OF>', {weight:'dist',default:10,x:'lon',y:'lat'}) YIELD path, weight - run A* with relationship property name as cost function")
    public Stream<WeightedPathResult> aStarConfig(
            @Name("startNode") Node startNode,
            @Name("endNode") Node endNode,
            @Name("relationshipTypesAndDirections") String relTypesAndDirs,
            @Name("config") Map<String,Object> config) {

        config = config == null ? Collections.emptyMap() : config;
        String relationshipCostPropertyKey = config.getOrDefault("weight", "distance").toString();
        double defaultCost = ((Number) config.getOrDefault("default", Double.MAX_VALUE)).doubleValue();
        String latPropertyName = config.getOrDefault("y", "latitude").toString();
        String lonPropertyName = config.getOrDefault("x", "longitude").toString();

        PathFinder<WeightedPath> algo = GraphAlgoFactory.aStar(
                buildPathExpander(relTypesAndDirs),
                CommonEvaluators.doubleCostEvaluator(relationshipCostPropertyKey, defaultCost),
                CommonEvaluators.geoEstimateEvaluator(latPropertyName, lonPropertyName));
        return streamWeightedPathResult(startNode, endNode, algo);
    }

    @Procedure
    @Description("apoc.algo.dijkstra(startNode, endNode, 'KNOWS|<WORKS_WITH|IS_MANAGER_OF>', 'distance') YIELD path, weight - run dijkstra with relationship property name as cost function")
    public Stream<WeightedPathResult> dijkstra(
            @Name("startNode") Node startNode,
            @Name("endNode") Node endNode,
            @Name("relationshipTypesAndDirections") String relTypesAndDirs,
            @Name("weightPropertyName") String weightPropertyName) {

        PathFinder<WeightedPath> algo = GraphAlgoFactory.dijkstra(
                buildPathExpander(relTypesAndDirs),
                weightPropertyName
        );
        return streamWeightedPathResult(startNode, endNode, algo);
    }


    @Procedure
    @Description("apoc.algo.allSimplePaths(startNode, endNode, 'KNOWS|<WORKS_WITH|IS_MANAGER_OF>', 5) YIELD path, weight - run allSimplePaths with relationships given and maxNodes")
    public Stream<PathResult> allSimplePaths(
            @Name("startNode") Node startNode,
            @Name("endNode") Node endNode,
            @Name("relationshipTypesAndDirections") String relTypesAndDirs,
            @Name("maxNodes") long maxNodes) {

        PathFinder<Path> algo = GraphAlgoFactory.allSimplePaths(
                buildPathExpander(relTypesAndDirs),
                (int) maxNodes
        );
        Iterable<Path> allPaths = algo.findAllPaths(startNode, endNode);
        return StreamSupport.stream(allPaths.spliterator(), false)
                .map(PathResult::new);
    }

    @Procedure
    @Description("apoc.algo.dijkstraWithDefaultWeight(startNode, endNode, 'KNOWS|<WORKS_WITH|IS_MANAGER_OF>', 'distance', 10) YIELD path, weight - run dijkstra with relationship property name as cost function and a default weight if the property does not exist")
    public Stream<WeightedPathResult> dijkstraWithDefaultWeight(
            @Name("startNode") Node startNode,
            @Name("endNode") Node endNode,
            @Name("relationshipTypesAndDirections") String relTypesAndDirs,
            @Name("weightPropertyName") String weightPropertyName,
            @Name("defaultWeight") double defaultWeight) {

        PathFinder<WeightedPath> algo = GraphAlgoFactory.dijkstra(
                buildPathExpander(relTypesAndDirs),
                (relationship, direction) -> ((Number) (relationship.getProperty(weightPropertyName, defaultWeight))).doubleValue()
        );
        return streamWeightedPathResult(startNode, endNode, algo);
    }

    private PathExpander<Object> buildPathExpander(String relationshipsAndDirections) {
        PathExpanderBuilder builder = PathExpanderBuilder.empty();
        for (Pair<RelationshipType, Direction> pair : RelationshipTypeAndDirections.parse(relationshipsAndDirections)) {
            if (pair.first()==null) {
                if (pair.other() == null ) {
                    builder = PathExpanderBuilder.allTypesAndDirections();
                }
                else {
                    builder = PathExpanderBuilder.allTypes(pair.other());
                }
            } else {
                if (pair.other() == null) {
                    builder = builder.add(pair.first());
                } else {
                    builder = builder.add(pair.first(), pair.other());
                }
            }
        }
        return builder.build();
    }

    private Stream<WeightedPathResult> streamWeightedPathResult(@Name("startNode") Node startNode, @Name("endNode") Node endNode, PathFinder<WeightedPath> algo) {
        Iterable<WeightedPath> allPaths = algo.findAllPaths(startNode, endNode);
        return StreamSupport.stream(allPaths.spliterator(), false)
                .map(WeightedPathResult::new);
    }

    public static class WeightedPathResult { // TODO: derive from PathResult when access to derived properties is fixed for yield
        public Path path;
        public double weight;

        public WeightedPathResult(WeightedPath weightedPath) {
            this.path = weightedPath;
            this.weight = weightedPath.weight();
        }
    }

    private static class DoubleEstimateEvaluator implements EstimateEvaluator<Double> {

        private final String xProperty;
        private final String yProperty;

        public DoubleEstimateEvaluator(String xProperty, String yProperty) {
            this.xProperty = xProperty;
            this.yProperty = yProperty;
        }

        @Override
        public Double getCost(final Node node, final Node goal) {
            double dx = doubleValue(node, xProperty) - doubleValue(goal, xProperty);
            double dy = doubleValue(node, yProperty) - doubleValue(goal, yProperty);
            return Math.sqrt(dx * dx + dy * dy);
        }
    }

    private static double doubleValue(PropertyContainer pc, String prop, Number defaultValue) {
        Object costProp = pc.getProperty(prop,defaultValue);
        if (costProp instanceof Number) return ((Number) costProp).doubleValue();
        return Double.parseDouble(costProp.toString());

    }
    private static double doubleValue(PropertyContainer pc, String prop) {
        return doubleValue(pc,prop,0);
    }
}
