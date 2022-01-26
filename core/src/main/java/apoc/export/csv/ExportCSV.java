package apoc.export.csv;

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

/**
 * @author mh
 * @since 22.05.16
 */
public class ExportCSV {
    @Context
    public Transaction tx;

    @Context
    public GraphDatabaseService db;

    @Context
    public TerminationGuard terminationGuard;

    @Context
    public ApocConfig apocConfig;

    @Context
    public Pools pools;

    public ExportCSV() {
    }

    @Procedure
    @Description("apoc.export.csv.all(file,config) - exports whole database as csv to the provided file")
    public Stream<ProgressInfo> all(@Name("file") String fileName, @Name("config") Map<String, Object> config) throws Exception {
        String source = String.format("database: nodes(%d), rels(%d)", Util.nodeCount(tx), Util.relCount(tx));
        return exportCsv(fileName, source, new DatabaseSubGraph(tx), new ExportConfig(config));
    }

    @Procedure
    @Description("apoc.export.csv.data(nodes,rels,file,config) - exports given nodes and relationships as csv to the provided file")
    public Stream<ProgressInfo> data(@Name("nodes") List<Node> nodes, @Name("rels") List<Relationship> rels, @Name("file") String fileName, @Name("config") Map<String, Object> config) throws Exception {
        ExportConfig exportConfig = new ExportConfig(config);
        preventBulkImport(exportConfig);
        String source = String.format("data: nodes(%d), rels(%d)", nodes.size(), rels.size());
        return exportCsv(fileName, source, new NodesAndRelsSubGraph(tx, nodes, rels), exportConfig);
    }
    @Procedure
    @Description("apoc.export.csv.graph(graph,file,config) - exports given graph object as csv to the provided file")
    public Stream<ProgressInfo> graph(@Name("graph") Map<String,Object> graph, @Name("file") String fileName, @Name("config") Map<String, Object> config) throws Exception {
        Collection<Node> nodes = (Collection<Node>) graph.get("nodes");
        Collection<Relationship> rels = (Collection<Relationship>) graph.get("relationships");
        String source = String.format("graph: nodes(%d), rels(%d)", nodes.size(), rels.size());
        return exportCsv(fileName, source, new NodesAndRelsSubGraph(tx, nodes, rels), new ExportConfig(config));
    }

    @Procedure
    @Description("apoc.export.csv.query(query,file,{config,...,params:{params}}) - exports results from the cypher statement as csv to the provided file")
    public Stream<ProgressInfo> query(@Name("query") String query, @Name("file") String fileName, @Name("config") Map<String, Object> config) throws Exception {
        ExportConfig exportConfig = new ExportConfig(config);
        preventBulkImport(exportConfig);
        Map<String,Object> params = config == null ? Collections.emptyMap() : (Map<String,Object>)config.getOrDefault("params", Collections.emptyMap());
        Result result = tx.execute(query,params);

        String source = String.format("statement: cols(%d)", result.columns().size());
        return exportCsv(fileName, source,result, exportConfig);
    }

    private void preventBulkImport(ExportConfig config) {
        if (config.isBulkImport()) {
            throw new RuntimeException("You can use the `bulkImport` only with apoc.export.all and apoc.export.csv.graph");
        }
    }

    private Stream<ProgressInfo> exportCsv(@Name("file") String fileName, String source, Object data, ExportConfig exportConfig) throws Exception {
        apocConfig.checkWriteAllowed(exportConfig, fileName);
        final String format = "csv";
        ProgressInfo progressInfo = new ProgressInfo(fileName, source, format);
        progressInfo.batchSize = exportConfig.getBatchSize();
        ProgressReporter reporter = new ProgressReporter(null, null, progressInfo);
        CsvFormat exporter = new CsvFormat(db);

        ExportFileManager cypherFileManager = FileManagerFactory
                .createFileManager(fileName, exportConfig.isBulkImport(), exportConfig);

        if (exportConfig.streamStatements()) {
            return ExportUtils.getProgressInfoStream(db, pools.getDefaultExecutorService(), terminationGuard, format, exportConfig, reporter, cypherFileManager,
                    (reporterWithConsumer) -> dump(data, exportConfig, reporterWithConsumer, cypherFileManager, exporter));
        } else {
            dump(data, exportConfig, reporter, cypherFileManager, exporter);
            return reporter.stream();
        }
    }

    private void dump(Object data, ExportConfig c, ProgressReporter reporter, ExportFileManager printWriter, CsvFormat exporter) {
        if (data instanceof SubGraph)
            exporter.dump((SubGraph)data,printWriter,reporter,c);
        if (data instanceof Result)
            exporter.dump((Result)data,printWriter,reporter,c);
    }
}
