package apoc.export.csv;

import apoc.Description;
import apoc.Pools;
import apoc.export.cypher.ExportCypher;
import apoc.export.util.ExportConfig;
import apoc.export.util.NodesAndRelsSubGraph;
import apoc.export.util.ProgressReporter;
import apoc.result.ProgressInfo;
import apoc.util.QueueBasedSpliterator;
import apoc.util.Util;
import org.neo4j.cypher.export.DatabaseSubGraph;
import org.neo4j.cypher.export.SubGraph;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Result;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.TerminationGuard;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Future;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static apoc.util.FileUtils.checkWriteAllowed;
import static apoc.util.FileUtils.getPrintWriter;

/**
 * @author mh
 * @since 22.05.16
 */
public class ExportCSV {
    @Context
    public GraphDatabaseService db;

    @Context
    public TerminationGuard terminationGuard;

    public ExportCSV(GraphDatabaseService db) {
        this.db = db;
    }

    public ExportCSV() {
    }

    @Procedure
    @Description("apoc.export.csv.all(file,config) - exports whole database as csv to the provided file")
    public Stream<ProgressInfo> all(@Name("file") String fileName, @Name("config") Map<String, Object> config) throws Exception {

        String source = String.format("database: nodes(%d), rels(%d)", Util.nodeCount(db), Util.relCount(db));
        return exportCsv(fileName, source, new DatabaseSubGraph(db), config);
    }

    @Procedure
    @Description("apoc.export.csv.data(nodes,rels,file,config) - exports given nodes and relationships as csv to the provided file")
    public Stream<ProgressInfo> data(@Name("nodes") List<Node> nodes, @Name("rels") List<Relationship> rels, @Name("file") String fileName, @Name("config") Map<String, Object> config) throws Exception {

        String source = String.format("data: nodes(%d), rels(%d)", nodes.size(), rels.size());
        return exportCsv(fileName, source, new NodesAndRelsSubGraph(db, nodes, rels), config);
    }
    @Procedure
    @Description("apoc.export.csv.graph(graph,file,config) - exports given graph object as csv to the provided file")
    public Stream<ProgressInfo> graph(@Name("graph") Map<String,Object> graph, @Name("file") String fileName, @Name("config") Map<String, Object> config) throws Exception {

        Collection<Node> nodes = (Collection<Node>) graph.get("nodes");
        Collection<Relationship> rels = (Collection<Relationship>) graph.get("relationships");
        String source = String.format("graph: nodes(%d), rels(%d)", nodes.size(), rels.size());
        return exportCsv(fileName, source, new NodesAndRelsSubGraph(db, nodes, rels), config);
    }

    @Procedure
    @Description("apoc.export.csv.query(query,file,{config,...,params:{params}}) - exports results from the cypher kernelTransaction as csv to the provided file")
    public Stream<ProgressInfo> query(@Name("query") String query, @Name("file") String fileName, @Name("config") Map<String, Object> config) throws Exception {
        Map<String,Object> params = config == null ? Collections.emptyMap() : (Map<String,Object>)config.getOrDefault("params", Collections.emptyMap());
        Result result = db.execute(query,params);

        String source = String.format("kernelTransaction: cols(%d)", result.columns().size());
        return exportCsv(fileName, source,result,config);
    }

    private Stream<ProgressInfo> exportCsv(@Name("file") String fileName, String source, Object data, Map<String,Object> config) throws Exception {
        checkWriteAllowed();
        ExportConfig c = new ExportConfig(config);
        ProgressInfo progressInfo = new ProgressInfo(fileName, source, "csv");
        progressInfo.batchSize = c.getBatchSize();
        ProgressReporter reporter = new ProgressReporter(null, null, progressInfo);
        PrintWriter printWriter = getPrintWriter(fileName, null);
        CsvFormat exporter = new CsvFormat(db);
        if (c.streamStatements()) {
            Future<Boolean> future = null;
            try {
                StringWriter writer = new StringWriter(10_000);
                final ArrayBlockingQueue<ProgressInfo> queue = new ArrayBlockingQueue<>(1000);
                ProgressReporter reporterWithConsumer = reporter.withConsumer(
                        (pi) -> queue.offer(pi == ProgressInfo.EMPTY ? ProgressInfo.EMPTY : new ProgressInfo(pi).drain(writer)));
                future = Util.inTxFuture(Pools.DEFAULT, db, () -> { dump(data, c, reporterWithConsumer, writer, exporter); return true; });
                QueueBasedSpliterator<ProgressInfo> spliterator = new QueueBasedSpliterator<>(queue, ProgressInfo.EMPTY, terminationGuard);
                return StreamSupport.stream(spliterator, false);
            } finally {
                Util.waitForFutures(Collections.singletonList(future));
            }
        } else {
            dump(data, c, reporter, printWriter, exporter);
            return reporter.stream();
        }
    }

    private void dump(Object data, ExportConfig c, ProgressReporter reporter, Writer printWriter, CsvFormat exporter) throws Exception {
        if (data instanceof SubGraph)
            exporter.dump(((SubGraph)data),printWriter,reporter,c);
        if (data instanceof Result)
            exporter.dump(((Result)data),printWriter,reporter,c);
    }
}
