package apoc.export.cypher;

import apoc.Description;
import apoc.export.util.ExportConfig;
import apoc.export.util.NodesAndRelsSubGraph;
import apoc.export.util.ProgressReporter;
import apoc.result.ProgressInfo;
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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static apoc.export.util.FileUtils.getPrintWriter;

/**
 * @author mh
 * @since 22.05.16
 */
public class ExportCypher {
    @Context
    public GraphDatabaseService db;

    public ExportCypher(GraphDatabaseService db) {
        this.db = db;
    }

    public ExportCypher() {
    }

    @Procedure
    @Description("apoc.export.cypher.all(file,config) - exports whole database incl. indexes as cypher statements to the provided file")
    public Stream<ProgressInfo> all(@Name("file") String fileName, @Name("config") Map<String, Object> config) throws IOException {

        String source = String.format("database: nodes(%d), rels(%d)", Util.nodeCount(db), Util.relCount(db));
        return exportCypher(fileName, source, new DatabaseSubGraph(db), new ExportConfig(config));
    }

    @Procedure
    @Description("apoc.export.cypher.data(nodes,rels,file,config) - exports given nodes and relationships incl. indexes as cypher statements to the provided file")
    public Stream<ProgressInfo> data(@Name("nodes") List<Node> nodes, @Name("rels") List<Relationship> rels, @Name("file") String fileName, @Name("config") Map<String, Object> config) throws IOException {

        String source = String.format("data: nodes(%d), rels(%d)", nodes.size(), rels.size());
        return exportCypher(fileName, source, new NodesAndRelsSubGraph(db, nodes, rels), new ExportConfig(config));
    }
    @Procedure
    @Description("apoc.export.cypher.graph(graph,file,config) - exports given graph object incl. indexes as cypher statements to the provided file")
    public Stream<ProgressInfo> graph(@Name("graph") Map<String,Object> graph, @Name("file") String fileName, @Name("config") Map<String, Object> config) throws IOException {

        Collection<Node> nodes = (Collection<Node>) graph.get("nodes");
        Collection<Relationship> rels = (Collection<Relationship>) graph.get("relationships");
        String source = String.format("graph: nodes(%d), rels(%d)", nodes.size(), rels.size());
        return exportCypher(fileName, source, new NodesAndRelsSubGraph(db, nodes, rels), new ExportConfig(config));
    }

    @Procedure
    @Description("apoc.export.cypher.query(query,file,config) - exports nodes and relationships from the cypher statement incl. indexes as cypher statements to the provided file")
    public Stream<ProgressInfo> query(@Name("query") String query, @Name("file") String fileName, @Name("config") Map<String, Object> config) throws IOException {
        ExportConfig c = new ExportConfig(config);
        Result result = db.execute(query);
        SubGraph graph = CypherResultSubGraph.from(result, db, c.getRelsInBetween());
        String source = String.format("statement: nodes(%d), rels(%d)",
                Iterables.count(graph.getNodes()), Iterables.count(graph.getRelationships()));
        return exportCypher(fileName, source, graph, c);
    }

    private Stream<ProgressInfo> exportCypher(@Name("file") String fileName, String source, SubGraph graph, ExportConfig c) throws IOException {
        ProgressReporter reporter = new ProgressReporter(null, null, new ProgressInfo(fileName, source, "cypher"));
        PrintWriter printWriter = getPrintWriter(fileName, null);
        MultiStatementCypherSubGraphExporter exporter = new MultiStatementCypherSubGraphExporter(graph);
        exporter.export(printWriter, c.getBatchSize(), reporter);
        return reporter.stream();
    }
}
