package apoc.export.cypher;

import apoc.ApocConfig;
import apoc.Pools;
import apoc.export.util.ExportConfig;
import apoc.export.util.NodesAndRelsSubGraph;
import apoc.export.util.ProgressReporter;
import apoc.result.ProgressInfo;
import apoc.util.QueueBasedSpliterator;
import apoc.util.QueueUtil;
import apoc.util.Util;
import org.neo4j.cypher.export.CypherResultSubGraph;
import org.neo4j.cypher.export.DatabaseSubGraph;
import org.neo4j.cypher.export.SubGraph;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.TerminationGuard;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * @author mh
 * @since 22.05.16
 */
public class ExportCypher {

    @Context
    public GraphDatabaseService db;

    @Context
    public Transaction tx;

    @Context
    public TerminationGuard terminationGuard;

    @Context
    public ApocConfig apocConfig;

    @Context
    public Pools pools;

    public ExportCypher(GraphDatabaseService db) {
        this.db = db;
    }

    public ExportCypher() {
    }

    @Procedure
    @Description("apoc.export.cypher.all(file,config) - exports whole database incl. indexes as cypher statements to the provided file")
    public Stream<DataProgressInfo> all(@Name(value = "file",defaultValue = "") String fileName, @Name(value = "config",defaultValue = "{}") Map<String, Object> config) throws IOException {
        if (Util.isNullOrEmpty(fileName)) fileName=null;
        String source = String.format("database: nodes(%d), rels(%d)", Util.nodeCount(tx), Util.relCount(tx));
        return exportCypher(fileName, source, new DatabaseSubGraph(tx), new ExportConfig(config), false);
    }

    @Procedure
    @Description("apoc.export.cypher.data(nodes,rels,file,config) - exports given nodes and relationships incl. indexes as cypher statements to the provided file")
    public Stream<DataProgressInfo> data(@Name("nodes") List<Node> nodes, @Name("rels") List<Relationship> rels, @Name(value = "file",defaultValue = "") String fileName, @Name(value = "config",defaultValue = "{}") Map<String, Object> config) throws IOException {
        if (Util.isNullOrEmpty(fileName)) fileName=null;
        String source = String.format("data: nodes(%d), rels(%d)", nodes.size(), rels.size());
        return exportCypher(fileName, source, new NodesAndRelsSubGraph(tx, nodes, rels), new ExportConfig(config), false);
    }

    @Procedure
    @Description("apoc.export.cypher.graph(graph,file,config) - exports given graph object incl. indexes as cypher statements to the provided file")
    public Stream<DataProgressInfo> graph(@Name("graph") Map<String, Object> graph, @Name(value = "file",defaultValue = "") String fileName, @Name(value = "config",defaultValue = "{}") Map<String, Object> config) throws IOException {
        if (Util.isNullOrEmpty(fileName)) fileName=null;

        Collection<Node> nodes = (Collection<Node>) graph.get("nodes");
        Collection<Relationship> rels = (Collection<Relationship>) graph.get("relationships");
        String source = String.format("graph: nodes(%d), rels(%d)", nodes.size(), rels.size());
        return exportCypher(fileName, source, new NodesAndRelsSubGraph(tx, nodes, rels), new ExportConfig(config), false);
    }

    @Procedure
    @Description("apoc.export.cypher.query(query,file,config) - exports nodes and relationships from the cypher statement incl. indexes as cypher statements to the provided file")
    public Stream<DataProgressInfo> query(@Name("query") String query, @Name(value = "file",defaultValue = "") String fileName, @Name(value = "config",defaultValue = "{}") Map<String, Object> config) throws IOException {
        if (Util.isNullOrEmpty(fileName)) fileName=null;
        ExportConfig c = new ExportConfig(config);
        Result result = tx.execute(query);
        SubGraph graph;
        try {
            graph = CypherResultSubGraph.from(tx, result, c.getRelsInBetween());
        } catch (IllegalStateException e) {
            throw new RuntimeException("Full-text indexes on relationships are not supported, please delete them in order to complete the process");
        }
        String source = String.format("statement: nodes(%d), rels(%d)",
                Iterables.count(graph.getNodes()), Iterables.count(graph.getRelationships()));
        return exportCypher(fileName, source, graph, c, false);
    }

    @Procedure
    @Description("apoc.export.cypher.schema(file,config) - exports all schema indexes and constraints to cypher")
    public Stream<DataProgressInfo> schema(@Name(value = "file",defaultValue = "") String fileName, @Name(value = "config",defaultValue = "{}") Map<String, Object> config) throws IOException {
        if (Util.isNullOrEmpty(fileName)) fileName=null;
        String source = String.format("database: nodes(%d), rels(%d)", Util.nodeCount(tx), Util.relCount(tx));
        return exportCypher(fileName, source, new DatabaseSubGraph(tx), new ExportConfig(config), true);
    }

    private Stream<DataProgressInfo> exportCypher(@Name("file") String fileName, String source, SubGraph graph, ExportConfig c, boolean onlySchema) throws IOException {
        apocConfig.checkWriteAllowed(c, fileName);

        ProgressInfo progressInfo = new ProgressInfo(fileName, source, "cypher");
        progressInfo.batchSize = c.getBatchSize();
        ProgressReporter reporter = new ProgressReporter(null, null, progressInfo);
        boolean separatedFiles = !onlySchema && c.separateFiles();
        ExportFileManager cypherFileManager = FileManagerFactory.createFileManager(fileName, separatedFiles);

        if (c.streamStatements()) {
            long timeout = c.getTimeoutSeconds();
            final BlockingQueue<DataProgressInfo> queue = new ArrayBlockingQueue<>(1000);
            ProgressReporter reporterWithConsumer = reporter.withConsumer(
                    (pi) -> QueueUtil.put(queue, pi == ProgressInfo.EMPTY ? DataProgressInfo.EMPTY : new DataProgressInfo(pi).enrich(cypherFileManager),timeout));
            Util.inTxFuture(null, pools.getDefaultExecutorService(), db,
                    txInThread -> { doExport(graph, c, onlySchema, reporterWithConsumer, cypherFileManager); return true; },
                    0, _ignored -> {}, _ignored -> QueueUtil.put(queue, DataProgressInfo.EMPTY, timeout));
            QueueBasedSpliterator<DataProgressInfo> spliterator = new QueueBasedSpliterator<>(queue, DataProgressInfo.EMPTY, terminationGuard, Integer.MAX_VALUE);
            return StreamSupport.stream(spliterator, false);
        } else {
            doExport(graph, c, onlySchema, reporter, cypherFileManager);
            return reporter.stream().map(DataProgressInfo::new).map((dpi) -> dpi.enrich(cypherFileManager));
        }
    }

    private void doExport(SubGraph graph, ExportConfig c, boolean onlySchema, ProgressReporter reporter, ExportFileManager cypherFileManager) {
        MultiStatementCypherSubGraphExporter exporter = new MultiStatementCypherSubGraphExporter(graph, c, db);

        if (onlySchema)
            exporter.exportOnlySchema(cypherFileManager);
        else
            exporter.export(c, reporter, cypherFileManager);
    }

    public static class DataProgressInfo {
        public final String file;
        public final long batches;
        public String source;
        public final String format;
        public long nodes;
        public long relationships;
        public long properties;
        public long time;
        public long rows;
        public long batchSize;
        public String cypherStatements;
        public String nodeStatements;
        public String relationshipStatements;
        public String schemaStatements;
        public String cleanupStatements;

        public DataProgressInfo(ProgressInfo pi) {
            this.file = pi.file;
            this.format = pi.format;
            this.source = pi.source;
            this.nodes = pi.nodes;
            this.relationships = pi.relationships;
            this.properties = pi.properties;
            this.time = pi.time;
            this.rows = pi.rows;
            this.batchSize = pi.batchSize;
            this.batches = pi.batches;
        }
        public DataProgressInfo enrich(ExportFileManager fileInfo) {
            cypherStatements = fileInfo.drain("cypher");
            nodeStatements = fileInfo.drain("nodes");
            relationshipStatements = fileInfo.drain("relationships");
            schemaStatements = fileInfo.drain("schema");
            cleanupStatements = fileInfo.drain("cleanup");
            return this;
        }
        public static final DataProgressInfo EMPTY = new DataProgressInfo(ProgressInfo.EMPTY);

    }
}
