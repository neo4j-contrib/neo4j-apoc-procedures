package apoc.result;

import apoc.algo.pagerank.PageRank;
import org.neo4j.graphdb.Node;

/**
 * @author mh
 * @since 09.10.16
 */
public class NodeScoreStats extends NodeScore {
    public final PageRank.PageRankStatistics stats;
    public NodeScoreStats(Node node, Double score, PageRank.PageRankStatistics stats) {
        super(node, score);
        this.stats = stats;
    }
}
