package apoc.algo;

import org.neo4j.procedure.*;
import apoc.Pools;
import apoc.algo.algorithms.*;
import apoc.result.NodeScore;
import apoc.util.Util;
import org.neo4j.graphalgo.impl.centrality.BetweennessCentrality;
import org.neo4j.graphalgo.impl.centrality.ClosenessCentrality;
import org.neo4j.graphalgo.impl.centrality.CostDivider;
import org.neo4j.graphalgo.impl.shortestpath.SingleSourceShortestPath;
import org.neo4j.graphalgo.impl.shortestpath.SingleSourceShortestPathDijkstra;
import org.neo4j.graphalgo.impl.util.DoubleAdder;
import org.neo4j.graphalgo.impl.util.DoubleComparator;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.stream.Stream;

import static apoc.algo.algorithms.AlgoUtils.SETTING_BATCH_SIZE;
import static apoc.algo.algorithms.AlgoUtils.SETTING_WEIGHTED;

public class Centrality {

    @Context
    public GraphDatabaseService db;

    @Context
    public Log log;

    @Context
    public GraphDatabaseAPI dbAPI;

    static final ExecutorService pool = Pools.DEFAULT;

    @Procedure("apoc.algo.betweenness")
    @Description("CALL apoc.algo.betweenness(['TYPE',...],nodes,BOTH) YIELD node, score - calculate betweenness " +
            "centrality for given nodes")
    public Stream<NodeScore> betweenness(
            @Name("types") List<String> types,
            @Name("nodes") List<Node> nodes,
            @Name("direction") String direction) {
        assertParametersNotNull(types, nodes);
        try {
            RelationshipType[] relationshipTypes = types.isEmpty()
                    ? Util.allRelationshipTypes(db)
                    : Util.toRelTypes(types);
            SingleSourceShortestPath<Double> sssp = new SingleSourceShortestPathDijkstra<>(
                    0.0,
                    null,
                    (relationship, dir) -> 1.0,
                    new DoubleAdder(),
                    new DoubleComparator(),
                    Util.parseDirection(direction),
                    relationshipTypes);
            BetweennessCentrality<Double> betweennessCentrality =
                    new BetweennessCentrality<>(sssp, new HashSet<>(nodes));

            return nodes.stream()
                    .map(node -> new NodeScore(node, betweennessCentrality.getCentrality(node)));
        } catch (Exception e) {
            String errMsg = "Error encountered while calculating centrality";
            log.error(errMsg, e);
            throw new RuntimeException(errMsg, e);
        }
    }


    @Procedure(value = "apoc.algo.betweennessCypher",mode = Mode.WRITE)
    @Description("CALL apoc.algo.betweennessCypher(node_cypher,rel_cypher,write) - calculates betweeness " +
    " centrality based on cypher input")
    public Stream<apoc.algo.algorithms.AlgorithmInterface.Statistics> betweennessCypher(
            @Name("config") Map<String, Object> config) {
        String nodeCypher = AlgoUtils.getCypher(config, AlgoUtils.SETTING_CYPHER_NODE, AlgoUtils.DEFAULT_CYPHER_NODE);
        String relCypher = AlgoUtils.getCypher(config, AlgoUtils.SETTING_CYPHER_REL, AlgoUtils.DEFAULT_CYPHER_REL);
        boolean shouldWrite = (boolean)config.getOrDefault(AlgoUtils.SETTING_WRITE, AlgoUtils.DEFAULT_PAGE_RANK_WRITE);
        Number weight = (Number) config.get(SETTING_WEIGHTED);
        Number batchSize = (Number) config.get(SETTING_BATCH_SIZE);
        int concurrency = ((Number) config.getOrDefault("concurrency",Pools.getNoThreadsInDefaultPool())).intValue();
        String property = (String) config.getOrDefault("property","betweenness_centrality");

        long beforeReading = System.currentTimeMillis();
        log.info("BetweennessCypher: Reading data into local ds");
        apoc.algo.algorithms.BetweennessCentrality betweennessCentrality =
                new apoc.algo.algorithms.BetweennessCentrality(dbAPI, pool, log);

        boolean success = betweennessCentrality.readNodeAndRelCypherData(
                relCypher, nodeCypher, weight, batchSize, concurrency);
        if (!success) {
            String errorMsg = "Failure while reading cypher queries. Make sure the results are ordered.";
            log.info(errorMsg);
            throw new RuntimeException(errorMsg);
        }
        long afterReading = System.currentTimeMillis();

        log.info("BetweennessCypher: Graph stored in local ds in " + (afterReading - beforeReading) + " milliseconds");
        log.info("BetweennessCypher: Number of nodes: " + betweennessCentrality.numberOfNodes());
        log.info("BetweennessCypher: Number of relationships: " + betweennessCentrality.numberOfRels());


        betweennessCentrality.computeUnweightedParallel();

        long afterComputation = System.currentTimeMillis();
        log.info("BetweennessCypher: Computations took " + (afterComputation - afterReading) + " milliseconds");

        if (shouldWrite) {
            betweennessCentrality.writeResultsToDB(property);
            long afterWrite = System.currentTimeMillis();
            log.info("BetweennessCypher: Writeback took " + (afterWrite - afterComputation) + " milliseconds");
        }

        return Stream.of(betweennessCentrality.getStatistics());

    }


    @Procedure("apoc.algo.closeness")
    @Description("CALL apoc.algo.closeness(['TYPE',...],nodes, INCOMING) YIELD node, score - calculate closeness " +
            "centrality for given nodes")
    public Stream<NodeScore> closeness(
            @Name("types") List<String> types,
            @Name("nodes") List<Node> nodes,
            @Name("direction") String direction) {
        assertParametersNotNull(types, nodes);
        try {
            RelationshipType[] relationshipTypes = types.isEmpty()
                    ? Util.allRelationshipTypes(db)
                    : Util.toRelTypes(types);
            SingleSourceShortestPath<Double> sssp = new SingleSourceShortestPathDijkstra<>(
                    0.0,
                    null,
                    (relationship, dir) -> 1.0,
                    new DoubleAdder(),
                    new DoubleComparator(),
                    Util.parseDirection(direction),
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
                    .map(node -> new NodeScore(node, closenessCentrality.getCentrality(node)));
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


}
