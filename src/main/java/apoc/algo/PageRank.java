package apoc.algo;

import apoc.Description;
import apoc.Pools;
import apoc.algo.pagerank.PageRankArrayStorageParallelSPI;
import apoc.result.NodeScore;
import apoc.util.Util;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.stream.Stream;

public class PageRank {

    private static final String SETTING_PAGE_RANK_ITERATIONS = "iterations";
    private static final String SETTING_PAGE_RANK_TYPES = "types";
    static final ExecutorService pool = Pools.DEFAULT;
    static final Long DEFAULT_PAGE_RANK_ITERATIONS = 20L;

    @Context
    public GraphDatabaseService db;

    @Context
    public Log log;


    @Procedure("apoc.algo.pageRank")
    @Description("CALL apoc.algo.pageRank(nodes) YIELD node, score - calculates page rank for given nodes")
    public Stream<NodeScore> pageRank(
            @Name("nodes") List<Node> nodes) {
        return innerPageRank(
                DEFAULT_PAGE_RANK_ITERATIONS,
                nodes);
    }

    @Procedure("apoc.algo.pageRankWithConfig")
    @Description(
            "CALL apoc.algo.pageRankWithConfig(nodes,{iterations:_,types:_}) YIELD node, score - calculates page rank" +
                    " for given nodes")
    public Stream<NodeScore> pageRankWithConfig(
            @Name("nodes") List<Node> nodes,
            @Name("config") Map<String, Object> config) {
        return innerPageRank(
                (Long) config.getOrDefault(SETTING_PAGE_RANK_ITERATIONS, DEFAULT_PAGE_RANK_ITERATIONS),
                nodes,
                Util.typesAndDirectionsToTypesArray((String) config.getOrDefault(SETTING_PAGE_RANK_TYPES, "")));
    }

    private Stream<NodeScore> innerPageRank(Long iterations, List<Node> nodes, RelationshipType... types) {
        try {
            PageRankArrayStorageParallelSPI pageRank = new PageRankArrayStorageParallelSPI(db, pool);
            pageRank.compute(iterations.intValue(), types);
            return nodes.stream().map(node -> new NodeScore(node, pageRank.getResult(node.getId())));
        } catch (Exception e) {
            String errMsg = "Error encountered while calculating page rank";
            log.error(errMsg, e);
            throw new RuntimeException(errMsg, e);
        }
    }
}
