package apoc.export.json;

import apoc.Pools;
import apoc.export.util.CountingReader;
import apoc.export.util.ProgressReporter;
import apoc.result.ProgressInfo;
import apoc.util.BinaryFileType;
import apoc.util.FileUtils;
import apoc.util.JsonUtil;
import apoc.util.Util;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.TerminationGuard;

import java.util.Map;
import java.util.Scanner;
import java.util.stream.Stream;

public class ImportJson {
    @Context
    public GraphDatabaseService db;

    @Context
    public Pools pools;

    // todo - il primo parametro può essere anche un byte[]
    
    @Context
    public TerminationGuard terminationGuard;

    @Procedure(value = "apoc.import.json", mode = Mode.WRITE)
    @Description("apoc.import.json(fileOrBinary,config) - imports the json list to the provided file")
    public Stream<ProgressInfo> all(@Name("fileOrBinary") Object fileOrBinary, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        ProgressInfo result =
                Util.inThread(pools, () -> {
                    ImportJsonConfig importJsonConfig = new ImportJsonConfig(config);
                    String file = null;
                    String source = "binary";
                    final boolean isFileUrl = importJsonConfig.isFileUrl();
                    if (isFileUrl) {
                        file =  (String) fileOrBinary;
                        source = "file";
                    }
                    ProgressReporter reporter = new ProgressReporter(null, null, new ProgressInfo(file, source, "json"));

                    try (final CountingReader reader = isFileUrl
                            ? FileUtils.readerFor(file) 
                            : BinaryFileType.valueOf(importJsonConfig.getBinary()).toInputStream(fileOrBinary, importJsonConfig.getBinaryCharset()).asReader();
                         final Scanner scanner = new Scanner(reader).useDelimiter("\n|\r");
                         JsonImporter jsonImporter = new JsonImporter(importJsonConfig, db, reporter)) {
                        while (scanner.hasNext() && !Util.transactionIsTerminated(terminationGuard)) {
                            Map<String, Object> row = JsonUtil.OBJECT_MAPPER.readValue(scanner.nextLine(), Map.class);
                            jsonImporter.importRow(row);
                        }
                    }

                    return reporter.getTotal();
                });
        return Stream.of(result);
    }
}
