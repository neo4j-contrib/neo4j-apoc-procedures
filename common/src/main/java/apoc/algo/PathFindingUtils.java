package apoc.algo;

import apoc.path.RelationshipTypeAndDirections;
import org.neo4j.graphalgo.EstimateEvaluator;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PathExpander;
import org.neo4j.graphdb.PathExpanderBuilder;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.values.storable.PointValue;

public class PathFindingUtils {
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
