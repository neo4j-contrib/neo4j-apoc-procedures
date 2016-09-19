package apoc.algo;

import org.neo4j.procedure.Description;
import apoc.Pools;
import apoc.algo.algorithms.AlgoUtils;
import apoc.algo.pagerank.PageRankArrayStorageParallelCypher;
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
    public GraphDatabaseAPI dbAPI;

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

    @Procedure("apoc.algo.pageRankWithCypher")
    @Description("CALL apoc.algo.pageRankWithCypher({iterations,node_cypher,rel_cypher,write}) - calculates page rank based on cypher input")
    public Stream<apoc.algo.pagerank.PageRank.PageRankStatistics> pageRankWithCypher(
            @Name("config") Map<String, Object> config) {
        Long iterations = (Long) config.getOrDefault(SETTING_PAGE_RANK_ITERATIONS, DEFAULT_PAGE_RANK_ITERATIONS);
        String nodeCypher = AlgoUtils.getCypher(config, AlgoUtils.SETTING_CYPHER_NODE, AlgoUtils.DEFAULT_CYPHER_NODE);
        String relCypher = AlgoUtils.getCypher(config, AlgoUtils.SETTING_CYPHER_REL, AlgoUtils.DEFAULT_CYPHER_REL);
        boolean shouldWrite = (boolean)config.getOrDefault(AlgoUtils.SETTING_WRITE, AlgoUtils.DEFAULT_PAGE_RANK_WRITE);

        long beforeReading = System.currentTimeMillis();
        log.info("Pagerank: Reading data into local ds");
        PageRankArrayStorageParallelCypher pageRank = new PageRankArrayStorageParallelCypher(dbAPI, pool, log);
        boolean success = pageRank.readNodeAndRelCypherData(
                relCypher, nodeCypher);
        if (!success) {
            String errorMsg = "Failure while reading cypher queries. Make sure the results are ordered.";
            log.info(errorMsg);
            throw new RuntimeException(errorMsg);
        }
        long afterReading = System.currentTimeMillis();

        log.info("Pagerank: Graph stored in local ds in " + (afterReading - beforeReading) + " milliseconds");
        log.info("Pagerank: Number of nodes: " + pageRank.numberOfNodes());
        log.info("Pagerank: Number of relationships: " + pageRank.numberOfRels());

        pageRank.compute(iterations.intValue());

        long afterComputation = System.currentTimeMillis();
        log.info("Pagerank: Computations took " + (afterComputation - afterReading) + " milliseconds");

        if (shouldWrite) {
            pageRank.writeResultsToDB();
            long afterWrite = System.currentTimeMillis();
            log.info("Pagerank: Writeback took " + (afterWrite - afterComputation) + " milliseconds");
        }
        return Stream.of(pageRank.getStatistics());
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
