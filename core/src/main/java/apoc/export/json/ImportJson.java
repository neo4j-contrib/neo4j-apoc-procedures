package apoc.export.json;

import apoc.Pools;
import apoc.export.util.CountingReader;
import apoc.export.util.ProgressReporter;
import apoc.result.ProgressInfo;
import apoc.util.FileUtils;
import apoc.util.JsonUtil;
import apoc.util.Util;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.Scanner;
import java.util.stream.Stream;

public class ImportJson {
    @Context
    public GraphDatabaseService db;

    @Context
    public Pools pools;

    @Procedure(value = "apoc.import.json", mode = Mode.WRITE)
    @Description("apoc.import.json(file,config) - imports the json list to the provided file")
    public Stream<ProgressInfo> all(@Name("file") String fileName, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        ProgressInfo result =
                Util.inThread(pools, () -> {
                    ImportJsonConfig importJsonConfig = new ImportJsonConfig(config);
                    ProgressReporter reporter = new ProgressReporter(null, null, new ProgressInfo(fileName, "file", "json"));

                    try (final CountingReader reader = FileUtils.readerFor(fileName);
                         final Scanner scanner = new Scanner(reader).useDelimiter("\n|\r");
                         JsonImporter jsonImporter = new JsonImporter(importJsonConfig, db, reporter)) {
                        while (scanner.hasNext()) {
                            Map<String, Object> row = JsonUtil.OBJECT_MAPPER.readValue(scanner.nextLine(), Map.class);
                            jsonImporter.importRow(row);
                        }
                    }

                    return reporter.getTotal();
                });
        return Stream.of(result);
    }
}
