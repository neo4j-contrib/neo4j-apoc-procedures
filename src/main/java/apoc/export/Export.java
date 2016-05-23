package apoc.export;

import apoc.Description;
import apoc.export.util.MultiStatementCypherSubGraphExporter;
import apoc.export.util.NodesAndRelsSubGraph;
import apoc.export.util.ProgressInfo;
import apoc.export.util.ProgressReporter;
import apoc.util.Util;
import org.neo4j.cypher.export.CypherResultSubGraph;
import org.neo4j.cypher.export.DatabaseSubGraph;
import org.neo4j.cypher.export.SubGraph;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Result;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static apoc.export.util.FileUtils.getPrintWriter;
import static apoc.util.Util.toBoolean;
import static apoc.util.Util.toLong;

/**
 * @author mh
 * @since 22.05.16
 */
public class Export {
    @Context
    public GraphDatabaseService db;

    @Procedure
    @Description("apoc.export.cypherAll(file,config) - exports whole database incl. indexes as cypher statements to the provided file")
    public Stream<ProgressInfo> cypherAll(@Name("file") String fileName, @Name("config") Map<String, Object> config) throws IOException {

        String source = String.format("database: nodes(%d), rels(%d)", Util.nodeCount(db), Util.relCount(db));
        return exportCypher(fileName, source, new DatabaseSubGraph(db), new Config(config));
    }

    @Procedure
    @Description("apoc.export.cypherData(nodes,rels,file,config) - exports given nodes and relationships incl. indexes as cypher statements to the provided file")
    public Stream<ProgressInfo> cypherData(@Name("nodes") List<Node> nodes, @Name("rels") List<Relationship> rels, @Name("file") String fileName, @Name("config") Map<String, Object> config) throws IOException {

        String source = String.format("data: nodes(%d), rels(%d)", nodes.size(), rels.size());
        return exportCypher(fileName, source, new NodesAndRelsSubGraph(db, nodes, rels), new Config(config));
    }

    @Procedure
    @Description("apoc.export.cypherQuery(query,file,config) - exports nodes and relationships from the cypher statement incl. indexes as cypher statements to the provided file")
    public Stream<ProgressInfo> cypherQuery(@Name("query") String query, @Name("file") String fileName, @Name("config") Map<String, Object> config) throws IOException {
        Config c = new Config(config);
        Result result = db.execute(query);
        SubGraph graph = CypherResultSubGraph.from(result, db, c.getRelsInBetween());
        String source = String.format("statement: nodes(%d), rels(%d)",
                Iterables.count(graph.getNodes()), Iterables.count(graph.getRelationships()));
        return exportCypher(fileName, source, graph, c);
    }

    private Stream<ProgressInfo> exportCypher(@Name("file") String fileName, String source, SubGraph graph, Config c) throws IOException {
        ProgressReporter reporter = new ProgressReporter(null, null, new ProgressInfo(fileName, source, "cypher"));
        PrintWriter printWriter = getPrintWriter(fileName, null);
        MultiStatementCypherSubGraphExporter exporter = new MultiStatementCypherSubGraphExporter(graph);
        exporter.export(printWriter, c.getBatchSize(), reporter);
        return reporter.stream();
    }

    static class Config {
        static final int DEFAULT_BATCH_SIZE = 20000;
        private final Map<String, Object> config;

        public Config(Map<String, Object> config) {
            this.config = config == null ? Collections.emptyMap() : config;
        }

        public boolean getRelsInBetween() {
            return toBoolean(config.get("nodesOfRelationships"));
        }

        public int getBatchSize() {
            return (int) toLong(config.getOrDefault("batchSize", DEFAULT_BATCH_SIZE));
        }
    }
}
