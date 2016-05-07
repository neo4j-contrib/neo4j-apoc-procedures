package apoc.algo;

import apoc.Description;
import apoc.Pools;
import apoc.algo.pagerank.PageRankArrayStorageParallelSPI;
import apoc.path.RelationshipTypeAndDirections;
import apoc.result.PathResult;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.neo4j.graphalgo.CommonEvaluators;
import org.neo4j.graphalgo.EstimateEvaluator;
import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphalgo.WeightedPath;
import org.neo4j.graphalgo.impl.centrality.BetweennessCentrality;
import org.neo4j.graphalgo.impl.centrality.ClosenessCentrality;
import org.neo4j.graphalgo.impl.centrality.CostDivider;
import org.neo4j.graphalgo.impl.shortestpath.SingleSourceShortestPath;
import org.neo4j.graphalgo.impl.shortestpath.SingleSourceShortestPathDijkstra;
import org.neo4j.graphalgo.impl.util.DoubleAdder;
import org.neo4j.graphalgo.impl.util.DoubleComparator;
import org.neo4j.graphdb.*;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.PerformsWrites;
import org.neo4j.procedure.Procedure;

import static java.lang.String.format;

public class Algo
{
    static final Long DEFAULT_PAGE_RANK_ITERATIONS = 20L;

    @Context
    public GraphDatabaseService db;

    @Context
    public GraphDatabaseAPI dbAPI;

    @Context
    public Log log;

    @Procedure
    @Description( "apoc.algo.aStar(startNode, endNode, 'KNOWS|<WORKS_WITH|IS_MANAGER_OF>', 'distance','lat','lon') " +
                  "YIELD path, weight - run A* with relationship property name as cost function" )
    public Stream<WeightedPathResult> aStar(
            @Name( "startNode" ) Node startNode,
            @Name( "endNode" ) Node endNode,
            @Name( "relationshipTypesAndDirections" ) String relTypesAndDirs,
            @Name( "weightPropertyName" ) String weightPropertyName,
            @Name( "latPropertyName" ) String latPropertyName,
            @Name( "lonPropertyName" ) String lonPropertyName )
    {

        PathFinder<WeightedPath> algo = GraphAlgoFactory.aStar(
                buildPathExpander( relTypesAndDirs ),
                CommonEvaluators.doubleCostEvaluator( weightPropertyName ),
                CommonEvaluators.geoEstimateEvaluator( latPropertyName, lonPropertyName ) );
        return streamWeightedPathResult( startNode, endNode, algo );
    }

    @Procedure
    @Description( "apoc.algo.aStar(startNode, endNode, 'KNOWS|<WORKS_WITH|IS_MANAGER_OF>', {weight:'dist',default:10," +
                  "x:'lon',y:'lat'}) YIELD path, weight - run A* with relationship property name as cost function" )
    public Stream<WeightedPathResult> aStarConfig(
            @Name( "startNode" ) Node startNode,
            @Name( "endNode" ) Node endNode,
            @Name( "relationshipTypesAndDirections" ) String relTypesAndDirs,
            @Name( "config" ) Map<String,Object> config )
    {

        config = config == null ? Collections.emptyMap() : config;
        String relationshipCostPropertyKey = config.getOrDefault( "weight", "distance" ).toString();
        double defaultCost = ((Number) config.getOrDefault( "default", Double.MAX_VALUE )).doubleValue();
        String latPropertyName = config.getOrDefault( "y", "latitude" ).toString();
        String lonPropertyName = config.getOrDefault( "x", "longitude" ).toString();

        PathFinder<WeightedPath> algo = GraphAlgoFactory.aStar(
                buildPathExpander( relTypesAndDirs ),
                CommonEvaluators.doubleCostEvaluator( relationshipCostPropertyKey, defaultCost ),
                CommonEvaluators.geoEstimateEvaluator( latPropertyName, lonPropertyName ) );
        return streamWeightedPathResult( startNode, endNode, algo );
    }

    @Procedure
    @Description( "apoc.algo.dijkstra(startNode, endNode, 'KNOWS|<WORKS_WITH|IS_MANAGER_OF>', 'distance') YIELD path," +
                  " weight - run dijkstra with relationship property name as cost function" )
    public Stream<WeightedPathResult> dijkstra(
            @Name( "startNode" ) Node startNode,
            @Name( "endNode" ) Node endNode,
            @Name( "relationshipTypesAndDirections" ) String relTypesAndDirs,
            @Name( "weightPropertyName" ) String weightPropertyName )
    {

        PathFinder<WeightedPath> algo = GraphAlgoFactory.dijkstra(
                buildPathExpander( relTypesAndDirs ),
                weightPropertyName
        );
        return streamWeightedPathResult( startNode, endNode, algo );
    }

    @Procedure
    @Description( "apoc.algo.allSimplePaths(startNode, endNode, 'KNOWS|<WORKS_WITH|IS_MANAGER_OF>', 5) YIELD path, " +
                  "weight - run allSimplePaths with relationships given and maxNodes" )
    public Stream<PathResult> allSimplePaths(
            @Name( "startNode" ) Node startNode,
            @Name( "endNode" ) Node endNode,
            @Name( "relationshipTypesAndDirections" ) String relTypesAndDirs,
            @Name( "maxNodes" ) long maxNodes )
    {

        PathFinder<Path> algo = GraphAlgoFactory.allSimplePaths(
                buildPathExpander( relTypesAndDirs ),
                (int) maxNodes
        );
        Iterable<Path> allPaths = algo.findAllPaths( startNode, endNode );
        return StreamSupport.stream( allPaths.spliterator(), false )
                .map( PathResult::new );
    }

    @Procedure
    @Description( "apoc.algo.dijkstraWithDefaultWeight(startNode, endNode, 'KNOWS|<WORKS_WITH|IS_MANAGER_OF>', " +
                  "'distance', 10) YIELD path, weight - run dijkstra with relationship property name as cost function" +
                  " and a default weight if the property does not exist" )
    public Stream<WeightedPathResult> dijkstraWithDefaultWeight(
            @Name( "startNode" ) Node startNode,
            @Name( "endNode" ) Node endNode,
            @Name( "relationshipTypesAndDirections" ) String relTypesAndDirs,
            @Name( "weightPropertyName" ) String weightPropertyName,
            @Name( "defaultWeight" ) double defaultWeight )
    {

        PathFinder<WeightedPath> algo = GraphAlgoFactory.dijkstra(
                buildPathExpander( relTypesAndDirs ),
                ( relationship, direction ) -> ((Number) (relationship
                        .getProperty( weightPropertyName, defaultWeight ))).doubleValue()
        );
        return streamWeightedPathResult( startNode, endNode, algo );
    }

    @Procedure( "apoc.algo.betweenness" )
    @Description( "CALL apoc.algo.betweenness(['TYPE',...],nodes,BOTH) YIELD node, score - calculate betweenness " +
                  "centrality for given nodes" )
    public Stream<NodeScore> betweenness(
            @Name( "types" ) List<String> types,
            @Name( "nodes" ) List<Node> nodes,
            @Name( "direction" ) String direction )
    {
        assertParametersNotNull( types, nodes );
        try
        {
            RelationshipType[] relationshipTypes = types.isEmpty()
                                                   ? allRelationshipTypes()
                                                   : stringsToRelationshipTypes( types );
            SingleSourceShortestPath<Double> sssp = new SingleSourceShortestPathDijkstra<>(
                    0.0,
                    null,
                    ( relationship, dir ) -> 1.0,
                    new DoubleAdder(),
                    new DoubleComparator(),
                    parseDirection( direction ),
                    relationshipTypes );
            BetweennessCentrality<Double> betweennessCentrality =
                    new BetweennessCentrality<>( sssp, new HashSet<>( nodes ) );

            return nodes.stream()
                    .map( node -> new NodeScore( node, betweennessCentrality.getCentrality( node ) ) );
        }
        catch ( Exception e )
        {
            String errMsg = "Error encountered while calculating centrality";
            log.error( errMsg, e );
            throw new RuntimeException( errMsg, e );
        }
    }

    @Procedure( "apoc.algo.closeness" )
    @Description( "CALL apoc.algo.closeness(['TYPE',...],nodes, INCOMING) YIELD node, score - calculate closeness " +
                  "centrality for given nodes" )
    public Stream<NodeScore> closeness(
            @Name( "types" ) List<String> types,
            @Name( "nodes" ) List<Node> nodes,
            @Name( "direction" ) String direction )
    {
        assertParametersNotNull( types, nodes );
        try
        {
            RelationshipType[] relationshipTypes = types.isEmpty()
                                                   ? allRelationshipTypes()
                                                   : stringsToRelationshipTypes( types );
            SingleSourceShortestPath<Double> sssp = new SingleSourceShortestPathDijkstra<>(
                    0.0,
                    null,
                    ( relationship, dir ) -> 1.0,
                    new DoubleAdder(),
                    new DoubleComparator(),
                    parseDirection( direction ),
                    relationshipTypes );

            ClosenessCentrality<Double> closenessCentrality =
                    new ClosenessCentrality<>( sssp, new DoubleAdder(), 0.0, new HashSet<>( nodes ),
                            new CostDivider<Double>()
                            {
                                @Override
                                public Double divideByCost( Double d, Double c )
                                {
                                    return d / c;
                                }

                                @Override
                                public Double divideCost( Double c, Double d )
                                {
                                    return c / d;
                                }
                            } );

            return nodes.stream()
                    .map( node -> new NodeScore( node, closenessCentrality.getCentrality( node ) ) );
        }
        catch ( Exception e )
        {
            String errMsg = "Error encountered while calculating centrality";
            log.error( errMsg, e );
            throw new RuntimeException( errMsg, e );
        }
    }

    @Procedure( "apoc.algo.pageRank" )
    @Description( "CALL apoc.algo.pageRank(nodes) YIELD node, score - calculates page rank for given nodes" )
    public Stream<NodeScore> pageRank( @Name( "nodes" ) List<Node> nodes )
    {
        return innerPageRank( DEFAULT_PAGE_RANK_ITERATIONS, nodes );
    }

    @Procedure( "apoc.algo.pageRankWithIterations" )
    @Description(
            "CALL apoc.algo.pageRankWithIterations(iterations, nodes) YIELD node, score - calculates page rank for " +
            "given nodes" )
    public Stream<NodeScore> pageRankWithIterations(
            @Name( "iterations" ) Long iterations,
            @Name( "nodes" ) List<Node> nodes )
    {
        return innerPageRank( iterations, nodes );
    }

    private Stream<NodeScore> innerPageRank( Long iterations, List<Node> nodes )
    {
        try
        {
            PageRankArrayStorageParallelSPI pageRank = new PageRankArrayStorageParallelSPI( db, Pools.DEFAULT);
            pageRank.compute( iterations.intValue() );
            return nodes.stream().map( node -> new NodeScore( node, pageRank.getResult( node.getId() ) ) );
        }
        catch ( Exception e )
        {
            String errMsg = "Error encountered while calculating page rank";
            log.error( errMsg, e );
            throw new RuntimeException( errMsg, e );
        }
    }

    @Procedure("apoc.algo.community")
    @PerformsWrites
    @Description("CALL apoc.algo.community(times,labels,partitionKey,type,direction,weightKey,batchSize) - simple label propagation kernel")
    public void community(
            @Name("times") long times,
            @Name("labels") List<String> labelNames,
            @Name("partitionKey") String partitionKey,
            @Name("type") String relationshipTypeName,
            @Name("direction") String directionName,
            @Name("weightKey") String weightKey,
            @Name("batchSize") long batchSize
    ) throws ExecutionException {
        Set<Label> labels = labelNames == null ? Collections.emptySet() : new HashSet<>(labelNames.size());
        if (labelNames != null)
            labelNames.forEach(name -> labels.add(Label.label(name)));

        RelationshipType relationshipType = relationshipTypeName == null ? null : RelationshipType.withName(relationshipTypeName);

        Direction direction = parseDirection(directionName);

        for (int i = 0; i < times; i++) {
            List<Node> batch = null;
            List<Future<Void>> futures = new ArrayList<>();
            try(Transaction tx = dbAPI.beginTx()) {
                for (Node node : dbAPI.getAllNodes()) {
                    boolean add = labels.size() == 0;
                    if (!add) {
                        Iterator<Label> nodeLabels = node.getLabels().iterator();
                        while (!add && nodeLabels.hasNext()) {
                            if (labels.contains(nodeLabels.next()))
                                add = true;
                        }
                    }
                    if (add) {
                        if (batch == null) {
                            batch = new ArrayList<>((int) batchSize);
                        }
                        batch.add(node);
                        if (batch.size() == batchSize) {
                            futures.add(clusterBatch(batch,partitionKey,relationshipType,direction,weightKey));
                            batch = null;
                        }
                    }
                }
                if (batch != null) {
                    futures.add(clusterBatch(batch,partitionKey,relationshipType,direction,weightKey));
                }
                tx.success();
            }

            // Await processing of node batches
            for (Future<Void> future : futures) {
                Pools.force(future);
            }
        }
    }

    private Future<Void> clusterBatch(List<Node> batch, String partitionKey, RelationshipType relationshipType, Direction direction, String weightKey) {
        return Pools.processBatch(batch, dbAPI, (node) -> {
           Map<Object,Double> votes = new HashMap<>();
           for (Relationship rel :
                   relationshipType == null
                   ? node.getRelationships(direction)
                   : node.getRelationships(relationshipType, direction)) {
               Node other = rel.getOtherNode(node);
               Object partition = partition(other, partitionKey);
               double weight = weight(rel, weightKey) * weight(other, weightKey);
               vote(votes, partition, weight);
           }

            Object originalPartition = partition(node, partitionKey);
            Object partition = originalPartition;
            double weight = 0.0d;
            for (Map.Entry<Object,Double> entry : votes.entrySet()) {
                if (weight < entry.getValue()) {
                    weight = entry.getValue();
                    partition = entry.getKey();
                }
            }

            if (partition != originalPartition)
                node.setProperty(partitionKey, partition);
        });
    }

    private void vote(Map<Object, Double> votes, Object partition, double weight) {
        double currentWeight = votes.getOrDefault(partition, 0.0d);
        double newWeight = currentWeight + weight;
        votes.put(partition, newWeight);
    }

    private double weight(PropertyContainer container, String propertyKey) {
        if (propertyKey != null) {
            Object propertyValue = container.getProperty(propertyKey, null);
            if (propertyValue instanceof Number) {
                return ((Number) propertyValue).doubleValue();
            }
        }
        return 1.0d;
    }

    private Object partition(Node node, String partitionKey) {
        Object partition = node.getProperty(partitionKey, null);
        return partition == null ? node.getId() : partition;
    }

    private Direction parseDirection( String direction )
    {
        if ( null == direction )
        {
            return Direction.BOTH;
        }
        try
        {
            return Direction.valueOf( direction.toUpperCase() );
        }
        catch ( Exception e )
        {
            throw new RuntimeException( format( "Cannot convert value '%s' to Direction. Legal values are '%s'",
                    direction, Arrays.toString( Direction.values() ) ) );
        }
    }

    private void assertParametersNotNull( List<String> types, List<Node> nodes )
    {
        if ( null == types || null == nodes )
        {
            String errMsg = "Neither 'types' nor 'nodes' procedure parameters may not be null.";
            if ( null == types )
            {
                errMsg += " 'types' is null";
            }
            if ( null == nodes )
            {
                errMsg += " 'nodes' is null";
            }
            log.error( errMsg );
            throw new RuntimeException( errMsg );
        }
    }

    private RelationshipType[] stringsToRelationshipTypes( List<String> relTypeStrings )
    {
        RelationshipType[] relTypes = new RelationshipType[relTypeStrings.size()];
        for ( int i = 0; i < relTypes.length; i++ )
        {
            relTypes[i] = RelationshipType.withName( relTypeStrings.get( i ) );
        }
        return relTypes;
    }

    private RelationshipType[] allRelationshipTypes()
    {
        return Iterables.asArray( RelationshipType.class, db.getAllRelationshipTypes() );
    }

    public static class NodeScore
    {
        public final Node node;
        public final Double score;

        public NodeScore( Node node, Double score )
        {
            this.node = node;
            this.score = score;
        }
    }

    private PathExpander<Object> buildPathExpander( String relationshipsAndDirections )
    {
        PathExpanderBuilder builder = PathExpanderBuilder.empty();
        for ( Pair<RelationshipType,Direction> pair : RelationshipTypeAndDirections
                .parse( relationshipsAndDirections ) )
        {
            if ( pair.first() == null )
            {
                if ( pair.other() == null )
                {
                    builder = PathExpanderBuilder.allTypesAndDirections();
                }
                else
                {
                    builder = PathExpanderBuilder.allTypes( pair.other() );
                }
            }
            else
            {
                if ( pair.other() == null )
                {
                    builder = builder.add( pair.first() );
                }
                else
                {
                    builder = builder.add( pair.first(), pair.other() );
                }
            }
        }
        return builder.build();
    }

    private Stream<WeightedPathResult> streamWeightedPathResult( @Name( "startNode" ) Node startNode,
            @Name( "endNode" ) Node endNode, PathFinder<WeightedPath> algo )
    {
        Iterable<WeightedPath> allPaths = algo.findAllPaths( startNode, endNode );
        return StreamSupport.stream( allPaths.spliterator(), false )
                .map( WeightedPathResult::new );
    }

    public static class WeightedPathResult
    { // TODO: derive from PathResult when access to derived properties is fixed for yield
        public Path path;
        public double weight;

        public WeightedPathResult( WeightedPath weightedPath )
        {
            this.path = weightedPath;
            this.weight = weightedPath.weight();
        }
    }

    private static class DoubleEstimateEvaluator implements EstimateEvaluator<Double>
    {

        private final String xProperty;
        private final String yProperty;

        public DoubleEstimateEvaluator( String xProperty, String yProperty )
        {
            this.xProperty = xProperty;
            this.yProperty = yProperty;
        }

        @Override
        public Double getCost( final Node node, final Node goal )
        {
            double dx = doubleValue( node, xProperty ) - doubleValue( goal, xProperty );
            double dy = doubleValue( node, yProperty ) - doubleValue( goal, yProperty );
            return Math.sqrt( dx * dx + dy * dy );
        }
    }

    private static double doubleValue( PropertyContainer pc, String prop, Number defaultValue )
    {
        Object costProp = pc.getProperty( prop, defaultValue );
        if ( costProp instanceof Number )
        {
            return ((Number) costProp).doubleValue();
        }
        return Double.parseDouble( costProp.toString() );

    }

    private static double doubleValue( PropertyContainer pc, String prop )
    {
        return doubleValue( pc, prop, 0 );
    }
}
