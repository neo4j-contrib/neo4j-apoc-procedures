package apoc.algo;

import apoc.path.RelationshipTypeAndDirections;
import apoc.result.PathResult;
import apoc.result.WeightedPathResult;
import apoc.util.Util;
import org.neo4j.graphalgo.*;
import org.neo4j.graphdb.*;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.values.storable.PointValue;

import java.util.Collections;
import java.util.Map;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class PathFinding {

    public static class GeoEstimateEvaluatorPointCustom implements EstimateEvaluator<Double> {

        // -- from org.neo4j.graphalgo.impl.util.GeoEstimateEvaluator
        private static final double EARTH_RADIUS = 6371 * 1000; // Meters
        private Node cachedGoal;
        private final String pointPropertyKey;
        private double[] cachedGoalCoordinates;
        
        public GeoEstimateEvaluatorPointCustom(String pointPropertyKey) {
            this.pointPropertyKey = pointPropertyKey;
        }
        
        @Override
        public Double getCost( Node node, Node goal) {
            double[] nodeCoordinates = getCoordinates(node);
            if ( cachedGoal == null || !cachedGoal.equals( goal ) )
            {
                cachedGoalCoordinates = getCoordinates(goal);
                cachedGoal = goal;
            }
            return distance(nodeCoordinates[0], nodeCoordinates[1],
                    cachedGoalCoordinates[0], cachedGoalCoordinates[1] );
        }
        
        private static double distance( double latitude1, double longitude1,
                                        double latitude2, double longitude2 ) {
            latitude1 = Math.toRadians( latitude1 );
            longitude1 = Math.toRadians( longitude1 );
            latitude2 = Math.toRadians( latitude2 );
            longitude2 = Math.toRadians( longitude2 );
            double cLa1 = Math.cos( latitude1 );
            double xA = EARTH_RADIUS * cLa1 * Math.cos( longitude1 );
            double yA = EARTH_RADIUS * cLa1 * Math.sin( longitude1 );
            double zA = EARTH_RADIUS * Math.sin( latitude1 );
            double cLa2 = Math.cos( latitude2 );
            double xB = EARTH_RADIUS * cLa2 * Math.cos( longitude2 );
            double yB = EARTH_RADIUS * cLa2 * Math.sin( longitude2 );
            double zB = EARTH_RADIUS * Math.sin( latitude2 );
            return Math.sqrt( ( xA - xB ) * ( xA - xB ) + ( yA - yB )
                    * ( yA - yB ) + ( zA - zB ) * ( zA - zB ) );
        }
        // -- end from org.neo4j.graphalgo.impl.util.GeoEstimateEvaluator

        private double[] getCoordinates(Node node) {
            return ((PointValue) node.getProperty(pointPropertyKey)).coordinate();
        }
    }

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
            estimateEvaluator = new GeoEstimateEvaluatorPointCustom(pointPropertyName);
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

    public static PathExpander<Double> buildPathExpander(String relationshipsAndDirections) {
        PathExpanderBuilder builder = PathExpanderBuilder.empty();
        for (Pair<RelationshipType, Direction> pair : RelationshipTypeAndDirections
                .parse(relationshipsAndDirections)) {
            if (pair.first() == null) {
                if (pair.other() == null) {
                    builder = PathExpanderBuilder.allTypesAndDirections();
                } else {
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

}
