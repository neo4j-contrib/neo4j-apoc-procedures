package apoc.load;

import apoc.Extended;
import apoc.Pools;
import apoc.export.util.CountingReader;
import apoc.export.util.ExportConfig;
import apoc.export.util.ProgressReporter;
import apoc.load.util.XmlReadUtil.Import;
import apoc.result.MapResult;
import apoc.result.ProgressInfo;
import apoc.util.FileUtils;
import apoc.util.Util;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.security.URLAccessChecker;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.TerminationGuard;

import java.util.Map;
import java.util.stream.Stream;

import static apoc.load.util.XmlReadUtil.Load.xmlXpathToMapResult;

@Extended
public class Gexf {

    @Context
    public GraphDatabaseService db;

    @Context
    public URLAccessChecker urlAccessChecker;

    @Context
    public TerminationGuard terminationGuard;

    @Context
    public Pools pools;

    @Procedure("apoc.load.gexf")
    @Description("apoc.load.gexf(urlOrBinary, path, $config) - load Gexf file from URL or binary source")
    public Stream<MapResult> gexf(
            @Name("urlOrBinary") Object urlOrBinary,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config
    ) throws Exception {
        return xmlXpathToMapResult(urlOrBinary, urlAccessChecker, terminationGuard, config);
    }

    @Procedure(name = "apoc.import.gexf", mode = Mode.WRITE)
    @Description("Imports a graph from the provided GraphML file.")
    public Stream<ProgressInfo> importGexf(
            @Name("urlOrBinaryFile") Object urlOrBinaryFile, @Name("config") Map<String, Object> config) {
        ProgressInfo result = Util.inThread(pools, () -> {
            ExportConfig exportConfig = new ExportConfig(config);
            String file = null;
            String source = "binary";
            if (urlOrBinaryFile instanceof String) {
                file = (String) urlOrBinaryFile;
                source = "file";
            }
            ProgressReporter reporter = new ProgressReporter(null, null, new ProgressInfo(file, source, "gexf"));
            Import graphReader = new Import(db)
                    .reporter(reporter)
                    .batchSize(exportConfig.getBatchSize())
                    .relType(exportConfig.defaultRelationshipType())
                    .source(exportConfig.getSource())
                    .target(exportConfig.getTarget())
                    .nodeLabels(exportConfig.readLabels());

            if (exportConfig.storeNodeIds()) graphReader.storeNodeIds();

            try (CountingReader reader =
                         FileUtils.readerFor(urlOrBinaryFile, exportConfig.getCompressionAlgo(), urlAccessChecker)) {
                graphReader.parseXML(reader, terminationGuard);
            }

            return reporter.getTotal();
        });
        return Stream.of(result);
    }
}
