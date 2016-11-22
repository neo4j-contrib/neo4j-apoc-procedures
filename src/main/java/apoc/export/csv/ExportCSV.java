package apoc.export.csv;

import apoc.Description;
import apoc.export.util.*;
import apoc.result.ProgressInfo;
import apoc.util.Util;
import au.com.bytecode.opencsv.CSVWriter;
import org.neo4j.cypher.export.DatabaseSubGraph;
import org.neo4j.cypher.export.SubGraph;
import org.neo4j.graphdb.*;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.io.PrintWriter;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static apoc.export.util.FileUtils.checkWriteAllowed;
import static apoc.export.util.FileUtils.getPrintWriter;

/**
 * @author mh
 * @since 22.05.16
 */
public class ExportCSV {
    @Context
    public GraphDatabaseService db;

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
    @Description("apoc.export.csv.query(query,file,config) - exports results from the cypher statement as csv to the provided file")
    public Stream<ProgressInfo> query(@Name("query") String query, @Name("file") String fileName, @Name("config") Map<String, Object> config) throws Exception {
        Result result = db.execute(query);

        String source = String.format("statement: cols(%d)", result.columns().size());
        return exportCsv(fileName, source,result,config);
    }

    private Stream<ProgressInfo> exportCsv(@Name("file") String fileName, String source, Object data, Map<String,Object> config) throws Exception {
        checkWriteAllowed();
        ExportConfig c = new ExportConfig(config);
        ProgressReporter reporter = new ProgressReporter(null, null, new ProgressInfo(fileName, source, "csv"));
        PrintWriter printWriter = getPrintWriter(fileName, null);
        CsvFormat exporter = new CsvFormat(db);
        if (data instanceof SubGraph)
            exporter.dump(((SubGraph)data),printWriter,reporter,c);
        if (data instanceof Result)
            exporter.dump(((Result)data),printWriter,reporter,c);
        return reporter.stream();
    }
}
