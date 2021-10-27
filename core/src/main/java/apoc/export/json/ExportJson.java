package apoc.export.json;

import apoc.ApocConfig;
import apoc.Pools;
import apoc.export.cypher.ExportFileManager;
import apoc.export.cypher.FileManagerFactory;
import apoc.export.util.ExportConfig;
import apoc.export.util.ExportUtils;
import apoc.export.util.NodesAndRelsSubGraph;
import apoc.export.util.ProgressReporter;
import apoc.result.ProgressInfo;
import apoc.util.Util;
import org.neo4j.cypher.export.DatabaseSubGraph;
import org.neo4j.cypher.export.SubGraph;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.TerminationGuard;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class ExportJson {
    @Context
    public Transaction tx;

    @Context
    public GraphDatabaseService db;

    @Context
    public ApocConfig apocConfig;

    @Context
    public Pools pools;

    @Context
    public TerminationGuard terminationGuard;

    public ExportJson(GraphDatabaseService db) {
        this.db = db;
    }

    public ExportJson() {
    }

    @Procedure
    @Description("apoc.export.json.all(file,config) - exports whole database as json to the provided file")
    public Stream<ProgressInfo> all(@Name("file") String fileName, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {

        String source = String.format("database: nodes(%d), rels(%d)", Util.nodeCount(tx), Util.relCount(tx));
        return exportJson(fileName, source, new DatabaseSubGraph(tx), config);
    }

    @Procedure
    @Description("apoc.export.json.data(nodes,rels,file,config) - exports given nodes and relationships as json to the provided file")
    public Stream<ProgressInfo> data(@Name("nodes") List<Node> nodes, @Name("rels") List<Relationship> rels, @Name("file") String fileName, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {

        String source = String.format("data: nodes(%d), rels(%d)", nodes.size(), rels.size());
        return exportJson(fileName, source, new NodesAndRelsSubGraph(tx, nodes, rels), config);
    }
    @Procedure
    @Description("apoc.export.json.graph(graph,file,config) - exports given graph object as json to the provided file")
    public Stream<ProgressInfo> graph(@Name("graph") Map<String,Object> graph, @Name("file") String fileName, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {

        Collection<Node> nodes = (Collection<Node>) graph.get("nodes");
        Collection<Relationship> rels = (Collection<Relationship>) graph.get("relationships");
        String source = String.format("graph: nodes(%d), rels(%d)", nodes.size(), rels.size());
        return exportJson(fileName, source, new NodesAndRelsSubGraph(tx, nodes, rels), config);
    }

    @Procedure
    @Description("apoc.export.json.query(query,file,{config,...,params:{params}}) - exports results from the cypher statement as json to the provided file")
    public Stream<ProgressInfo> query(@Name("query") String query, @Name("file") String fileName, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        Map<String,Object> params = config == null ? Collections.emptyMap() : (Map<String,Object>)config.getOrDefault("params", Collections.emptyMap());
        Result result = tx.execute(query,params);
        String source = String.format("statement: cols(%d)", result.columns().size());
        return exportJson(fileName, source,result,config);
    }

    private Stream<ProgressInfo> exportJson(String fileName, String source, Object data, Map<String,Object> config) throws Exception {
        ExportConfig exportConfig = new ExportConfig(config);
        apocConfig.checkWriteAllowed(exportConfig, fileName);
        final String format = "json";
        ProgressReporter reporter = new ProgressReporter(null, null, new ProgressInfo(fileName, source, format));
        JsonFormat exporter = new JsonFormat(db, getJsonFormat(config));
        ExportFileManager cypherFileManager = FileManagerFactory.createFileManager(fileName, false);
        if (exportConfig.streamStatements()) {
            return ExportUtils.getProgressInfoStream(db, pools.getDefaultExecutorService() ,terminationGuard, format, exportConfig, reporter, cypherFileManager,
                    (reporterWithConsumer) -> dump(data, exportConfig, reporterWithConsumer, exporter, cypherFileManager));
        } else {
            dump(data, exportConfig, reporter, exporter, cypherFileManager);
            return reporter.stream();
        }
    }

    private JsonFormat.Format getJsonFormat(Map<String, Object> config) {
        if (config == null) {
            return JsonFormat.Format.JSON_LINES;
        }
        final String jsonFormat = config.getOrDefault("jsonFormat", JsonFormat.Format.JSON_LINES.toString())
                .toString()
                .toUpperCase();
        return JsonFormat.Format.valueOf(jsonFormat);
    }

    private void dump(Object data, ExportConfig c, ProgressReporter reporter, JsonFormat exporter, ExportFileManager cypherFileManager) {
        try {
            if (data instanceof SubGraph)
                exporter.dump(((SubGraph)data),cypherFileManager,reporter,c);
            if (data instanceof Result)
                exporter.dump(((Result)data),cypherFileManager,reporter,c);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
