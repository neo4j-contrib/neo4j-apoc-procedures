package apoc.export.csv;

import apoc.Description;
import apoc.export.util.ExportConfig;
import apoc.export.util.ProgressReporter;
import apoc.result.ProgressInfo;
import apoc.util.FileUtils;
import apoc.util.Util;
import org.neo4j.cypher.export.DatabaseSubGraph;
import org.neo4j.cypher.export.SubGraph;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.io.PrintWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class ImportCsv {
    @Context
    public GraphDatabaseService db;

    public ImportCsv(GraphDatabaseService db) {
        this.db = db;
    }

    public ImportCsv() {
    }

    @Procedure(name = "apoc.import.csv", mode = Mode.SCHEMA)
    @Description("apoc.import.csv(nodes, relationships, config) - imports nodes and relationships from the provided CSV files with given labels and types")
    public Stream<ProgressInfo> importCsv(
            @Name("nodes") List<Map<String, Object>> nodes,
            @Name("relationships") List<Map<String, String>> relationships,
            @Name("config") Map<String, Object> config
    ) throws Exception {
        final CsvLoaderConfig clc = CsvLoaderConfig.from(config);

        final Map<String, Map<String, Long>> idMapping = new HashMap<>();
        for (Map<String, Object> node: nodes) {
            final String fileName = (String) node.get("fileName");
            final List<String> labels = (List<String>) node.get("labels");
            CsvEntityLoader.loadNodes(fileName, labels, clc, db, idMapping);
        }

        for (Map<String, String> relationship: relationships) {
            final String fileName = relationship.get("fileName");
            final String type = relationship.get("type");
            CsvEntityLoader.loadRelationships(fileName, type, clc, db, idMapping);
        }

        String source = String.format("database: nodes(%d), rels(%d)", Util.nodeCount(db), Util.relCount(db));
        return reportProgress(source, new DatabaseSubGraph(db), config);
    }


    private Stream<ProgressInfo> reportProgress(String source, Object data, Map<String, Object> config) throws Exception {
        String fileName = "progress.csv";

        FileUtils.checkWriteAllowed();
        ExportConfig c = new ExportConfig(config);
        ProgressReporter reporter = new ProgressReporter(null, null, new ProgressInfo(fileName, source, "csv"));
        PrintWriter printWriter = FileUtils.getPrintWriter(fileName, null);
        CsvFormat exporter = new CsvFormat(db);
        if (data instanceof SubGraph)
            exporter.dump(((SubGraph) data), printWriter, reporter, c);
        if (data instanceof Result)
            exporter.dump(((Result) data), printWriter, reporter, c);
        return reporter.stream();
    }

}
