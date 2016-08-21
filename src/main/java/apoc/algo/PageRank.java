package apoc.algo;

import apoc.Description;
import apoc.Pools;
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
    private static final String SETTING_PAGE_RANK_CYPHER_NODE = "node_cypher";
    private static final String SETTING_PAGE_RANK_CYPHER_REL = "rel_cypher";
    private static final String SETTING_PAGE_RANK_TYPES = "types";
    private static final String SETTING_PAGE_RANK_WRITE = "write";

    static final ExecutorService pool = Pools.DEFAULT;
    static final Long DEFAULT_PAGE_RANK_ITERATIONS = 20L;
    static final String DEFAULT_PAGE_RANK_CYPHER_REL =
            "MATCH (s)-[r]->(t) RETURN id(s) as source, id(t) as target, 1 as weight";
    static final String DEFAULT_PAGE_RANK_CYPHER_NODE =
            "MATCH (s) RETURN id(s) as id";

    static final boolean DEFAULT_PAGE_RANK_WRITE = false;

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
        String nodeCypher = getCypher(config, SETTING_PAGE_RANK_CYPHER_NODE, DEFAULT_PAGE_RANK_CYPHER_NODE);
        String relCypher = getCypher(config, SETTING_PAGE_RANK_CYPHER_REL, DEFAULT_PAGE_RANK_CYPHER_REL);
        boolean shouldWrite = (boolean)config.getOrDefault(SETTING_PAGE_RANK_WRITE, DEFAULT_PAGE_RANK_WRITE);

        long beforeReading = System.currentTimeMillis();
        log.info("Pagerank: Reading data into local ds");
        PageRankArrayStorageParallelCypher pageRank = new PageRankArrayStorageParallelCypher(dbAPI, pool, log);
        boolean success = pageRank.readDataIntoArray(relCypher, nodeCypher);
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

    private String getCypher(Map<String, Object> config, String setting, String defaultString) {
        String cypher = (String)config.getOrDefault(setting, defaultString);
        if (cypher.equals("") || cypher.isEmpty()) {
            cypher = defaultString;
        }
        return cypher;
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
