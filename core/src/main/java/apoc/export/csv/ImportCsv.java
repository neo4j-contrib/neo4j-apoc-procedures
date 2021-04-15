package apoc.export.csv;

import apoc.Pools;
import apoc.export.util.ProgressReporter;
import apoc.result.ProgressInfo;
import apoc.util.Util;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class ImportCsv {
    @Context
    public GraphDatabaseService db;

    @Context
    public Pools pools;

    @Context
    public Log log;

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
        ProgressInfo result =
                Util.inThread(pools, () -> {
                    final ProgressReporter reporter = new ProgressReporter(null, null, new ProgressInfo("progress.csv", "file", "csv"));

                    final CsvLoaderConfig clc = CsvLoaderConfig.from(config);
                    final CsvEntityLoader loader = new CsvEntityLoader(clc, reporter, log);

                    final Map<String, Map<String, Long>> idMapping = new HashMap<>();
                    for (Map<String, Object> node : nodes) {
                        final String fileName = (String) node.get("fileName");
                        final List<String> labels = (List<String>) node.get("labels");
                        loader.loadNodes(fileName, labels, db, idMapping);
                    }

                    for (Map<String, String> relationship : relationships) {
                        final String fileName = relationship.get("fileName");
                        final String type = relationship.get("type");
                        loader.loadRelationships(fileName, type, db, idMapping);
                    }

                    return reporter.getTotal();
                });
        return Stream.of(result);
    }


}
