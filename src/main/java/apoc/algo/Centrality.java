package apoc.algo;

import apoc.Description;
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
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.HashSet;
import java.util.List;
import java.util.stream.Stream;

public class Centrality {

    @Context
    public GraphDatabaseService db;

    @Context
    public Log log;

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
