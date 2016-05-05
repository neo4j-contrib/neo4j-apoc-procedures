package apoc.algo;

import apoc.Description;
import apoc.path.RelationshipTypeAndDirections;
import apoc.result.PathResult;
import org.neo4j.graphalgo.*;
import org.neo4j.graphalgo.impl.centrality.BetweennessCentrality;
import org.neo4j.graphalgo.impl.centrality.ClosenessCentrality;
import org.neo4j.graphalgo.impl.centrality.CostDivider;
import org.neo4j.graphalgo.impl.shortestpath.SingleSourceShortestPath;
import org.neo4j.graphalgo.impl.shortestpath.SingleSourceShortestPathBFS;
import org.neo4j.graphalgo.impl.shortestpath.SingleSourceShortestPathDijkstra;
import org.neo4j.graphalgo.impl.util.DoubleAdder;
import org.neo4j.graphalgo.impl.util.DoubleComparator;
import org.neo4j.graphdb.*;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.lang.String.format;

public class Algo {

    @Context
    public GraphDatabaseService db;

    @Context
    public Log log;

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
                CommonEvaluators.geoEstimateEvaluator(latPropertyName, lonPropertyName));
        return streamWeightedPathResult(startNode, endNode, algo);
    }

    @Procedure
    @Description("apoc.algo.aStar(startNode, endNode, 'KNOWS|<WORKS_WITH|IS_MANAGER_OF>', {weight:'dist',default:10,x:'lon',y:'lat'}) YIELD path, weight - run A* with relationship property name as cost function")
    public Stream<WeightedPathResult> aStarConfig(
            @Name("startNode") Node startNode,
            @Name("endNode") Node endNode,
            @Name("relationshipTypesAndDirections") String relTypesAndDirs,
            @Name("config") Map<String, Object> config) {

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
                (relationship, direction) -> ((Number) (relationship
                        .getProperty(weightPropertyName, defaultWeight))).doubleValue()
        );
        return streamWeightedPathResult(startNode, endNode, algo);
    }

    @Procedure("apoc.algo.betweenness")
    @Description("CALL apoc.algo.betweenness(['TYPE',...],nodes,BOTH) YIELD node, centrality - calculate betweenness centrality for given nodes")
    public Stream<NodeCentrality> betweenness(@Name("types") List<String> types, @Name("nodes") List<Node> nodes
            , @Name("direction") String direction) {
        assertParametersNotNull(types, nodes);
        try {
            RelationshipType[] relationshipTypes = types.isEmpty()
                    ? allRelationshipTypes()
                    : stringsToRelationshipTypes(types);
            SingleSourceShortestPath<Integer> sssp = new SingleSourceShortestPathBFS(
                    null,
                    parseDirection(direction),
                    relationshipTypes);

            BetweennessCentrality<Integer> betweennessCentrality =
                    new BetweennessCentrality<>(sssp, new HashSet<>(nodes));

            return nodes.stream()
                    .map(node -> new NodeCentrality(node, betweennessCentrality.getCentrality(node)));
        } catch (Exception e) {
            String errMsg = "Error encountered while calculating centrality";
            log.error(errMsg, e);
            throw new RuntimeException(errMsg, e);
        }
    }

    private Direction parseDirection(String direction) {
        if (null == direction) {
            throw new RuntimeException("Direction cannot be null");
        }
        try {
            return Direction.valueOf(direction.toUpperCase());
        } catch (Exception e) {
            throw new RuntimeException(format("Cannot convert value '%s' to Direction. Legal values are '%s'",
                    direction, Arrays.toString(Direction.values())));
        }
    }

    @Procedure("apoc.algo.closeness")
    @Description("CALL apoc.algo.closeness(['TYPE',...],nodes, INCOMING) YIELD node, centrality - calculate closeness centrality for given nodes")
    public Stream<NodeCentrality> closeness(@Name("types") List<String> types, @Name("nodes") List<Node> nodes,
                                            @Name("direction") String direction) {
        assertParametersNotNull(types, nodes);
        try {
            RelationshipType[] relationshipTypes = types.isEmpty()
                    ? allRelationshipTypes()
                    : stringsToRelationshipTypes(types);
            SingleSourceShortestPath<Double> sssp = new SingleSourceShortestPathDijkstra<>(
                    0.0,
                    null,
                    (relationship, dir) -> 1.0, new DoubleAdder(),
                    new DoubleComparator(),
                    parseDirection(direction),
                    relationshipTypes);

            ClosenessCentrality<Double> closenessCentrality =
                    new ClosenessCentrality<>(sssp, new DoubleAdder(), 0.0, new HashSet<>(nodes),
                            new CostDivider<Double>() {
                                @Override
                                public Double divideByCost(Double d, Double c) {
                                    return d / c;
                                }

                                @Override
                                public Double divideCost(Double c, Double d) {
                                    return c / d;
                                }
                            });

            return nodes.stream()
                    .map(node -> new NodeCentrality(node, closenessCentrality.getCentrality(node)));
        } catch (Exception e) {
            String errMsg = "Error encountered while calculating centrality";
            log.error(errMsg, e);
            throw new RuntimeException(errMsg, e);
        }
    }

    private void assertParametersNotNull(List<String> types, List<Node> nodes) {
        if (null == types || null == nodes) {
            String errMsg = "Neither 'types' nor 'nodes' procedure parameters may not be null.";
            if (null == types) {
                errMsg += " 'types' is null";
            }
            if (null == nodes) {
                errMsg += " 'nodes' is null";
            }
            log.error(errMsg);
            throw new RuntimeException(errMsg);
        }
    }

    private RelationshipType[] stringsToRelationshipTypes(List<String> relTypeStrings) {
        RelationshipType[] relTypes = new RelationshipType[relTypeStrings.size()];
        for (int i = 0; i < relTypeStrings.size(); i++) {
            relTypes[i] = RelationshipType.withName(relTypeStrings.get(i));
        }
        return relTypes;
    }

    private RelationshipType[] allRelationshipTypes() {
        List<RelationshipType> relationshipTypes = Iterables.asList(db.getAllRelationshipTypes());
        return relationshipTypes.toArray(new RelationshipType[relationshipTypes.size()]);
    }

    public static class NodeCentrality {
        public final Node node;
        public final Double centrality;

        public NodeCentrality(Node node, Double centrality) {
            this.node = node;
            this.centrality = centrality;
        }
    }

    private PathExpander<Object> buildPathExpander(String relationshipsAndDirections) {
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

    private Stream<WeightedPathResult> streamWeightedPathResult(@Name("startNode") Node startNode,
                                                                @Name("endNode") Node endNode, PathFinder<WeightedPath> algo) {
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
        Object costProp = pc.getProperty(prop, defaultValue);
        if (costProp instanceof Number) {
            return ((Number) costProp).doubleValue();
        }
        return Double.parseDouble(costProp.toString());

    }

    private static double doubleValue(PropertyContainer pc, String prop) {
        return doubleValue(pc, prop, 0);
    }
}
