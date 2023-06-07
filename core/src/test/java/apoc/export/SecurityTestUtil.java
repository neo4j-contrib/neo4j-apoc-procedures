package apoc.export;

import apoc.ApocConfig;
import apoc.util.TestUtil;
import apoc.util.Util;
import com.ctc.wstx.exc.WstxUnexpectedCharException;
import com.fasterxml.jackson.core.JsonParseException;
import junit.framework.TestCase;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.QueryExecutionException;
import org.xml.sax.SAXParseException;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static apoc.ApocConfig.APOC_EXPORT_FILE_ENABLED;
import static apoc.ApocConfig.APOC_IMPORT_FILE_ALLOW__READ__FROM__FILESYSTEM;
import static apoc.ApocConfig.APOC_IMPORT_FILE_ENABLED;
import static apoc.ApocConfig.APOC_IMPORT_FILE_USE_NEO4J_CONFIG;
import static apoc.ApocConfig.apocConfig;
import static apoc.util.TestUtil.assertError;

public class SecurityTestUtil {
    public static final String PROCEDURE_KEY = "procedure";
    public static final String ERROR_KEY = "error";

    public static final Map<String, Class<?>> ALLOWED_EXCEPTIONS = Map.of(
            // load allowed exception
            "apoc.load.json", JsonParseException.class,
            "apoc.load.jsonArray", JsonParseException.class,
            "apoc.load.jsonParams", JsonParseException.class,
            "apoc.load.xml", SAXParseException.class,

            // import allowed exception
            "apoc.import.json", JsonParseException.class,
            "apoc.import.csv", RuntimeException.class,
            "apoc.import.graphml", JsonParseException.class,
            "apoc.import.xml", WstxUnexpectedCharException.class
    );

    public static List<Pair<String, String>> EXPORT_PROCEDURES = List.of(
            Pair.of("query", "'RETURN 1', $fileName, {}"),
            Pair.of("all", "$fileName, {}"),
            Pair.of("data", "[], [], $fileName, {}"),
            Pair.of("graph", "{nodes: [], relationships: []}, $fileName, {}")
    );

    public static Stream<Pair<String, String>> IMPORT_PROCEDURES = Stream.of(
            Pair.of("json", "($fileName)"),
            Pair.of("csv", "([{fileName: $fileName, labels: ['Person']}], [], {})"),
            Pair.of("csv", "([], [{fileName: $fileName, type: 'KNOWS'}], {})"),
            Pair.of("graphml", "($fileName, {})"),
            Pair.of("xml", "($fileName)")
    );

    public static Stream<Pair<String, String>> LOAD_PROCEDURES = Stream.of(
            Pair.of("json", "($fileName, '', {})"),
            Pair.of("jsonArray", "($fileName, '', {})"),
            Pair.of("jsonParams", "($fileName, {}, '')"),
            Pair.of("xml", "($fileName, '', {}, false)"),
            Pair.of("arrow", "($fileName)")
    );


    public static void assertPathTraversalError(GraphDatabaseService db, String query, Map<String, Object> params, Consumer<Map> exceptionConsumer) {

        QueryExecutionException e = Assert.assertThrows(QueryExecutionException.class,
                () -> TestUtil.testCall(db, query, params, (r) -> {})
        );


        // this consumer accept a Map.of("error", <errorMessage>, "procedure", <procedureQuery>)
        exceptionConsumer.accept(
                Util.map(ERROR_KEY, e, PROCEDURE_KEY, query)
        );
    }

    public static String getApocProcedure(String exportMethod, String exportMethodType, String exportMethodArguments) {
        return "CALL apoc.export." + exportMethod + "." + exportMethodType + "(" + exportMethodArguments + ")";
    }

    public static void testsWithExportDisabled(GraphDatabaseService db, String apocProcedure, String fileName) {
        testsWithExportDisabled(db, apocProcedure, fileName, ApocConfig.EXPORT_TO_FILE_ERROR);
    }

    public static void testsWithExportDisabled(GraphDatabaseService db, String apocProcedure, String fileName, String error) {
        // all assertions with `apoc.export.file.enabled=true`

        // apoc.import.file.use_neo4j_config=true
        // apoc.import.file.allow_read_from_filesystem=true
        setExportFileApocConfigs(false, true, true);
        assertExportDisabled(db, apocProcedure, fileName, error);

        // apoc.import.file.use_neo4j_config=true
        // apoc.import.file.allow_read_from_filesystem=false
        setExportFileApocConfigs(false, true, false);
        assertExportDisabled(db, apocProcedure, fileName, error);

        // apoc.import.file.use_neo4j_config=false
        // apoc.import.file.allow_read_from_filesystem=true
        setExportFileApocConfigs(false, false, true);
        assertExportDisabled(db, apocProcedure, fileName, error);

        // apoc.import.file.use_neo4j_config=false
        // apoc.import.file.allow_read_from_filesystem=false
        setExportFileApocConfigs(false, false, false);
        assertExportDisabled(db, apocProcedure, fileName, error);
    }

    private static void assertExportDisabled(GraphDatabaseService db, String apocProcedure, String fileName, String error) {
        QueryExecutionException e = Assert.assertThrows(QueryExecutionException.class,
                () -> TestUtil.testCall(db, apocProcedure, Map.of("fileName", fileName), (r) -> {})
        );

        assertError(e, error, RuntimeException.class, apocProcedure);
    }

    public static void assertPathTraversalWithoutErrors(GraphDatabaseService db, String apocProcedure, String fileName, File file) {
        TestUtil.testCall(db, apocProcedure, Map.of("fileName", fileName), r -> {});

        TestCase.assertTrue("The file doesn't exists", file.exists());
        TestCase.assertTrue(file.delete());
    }

    public static void setImportFileApocConfigs(boolean importEnabled, boolean useNeo4jConfs, boolean allowReadFromFs) {
        apocConfig().setProperty(APOC_IMPORT_FILE_ENABLED, importEnabled);
        setFileConfigsCommon(useNeo4jConfs, allowReadFromFs);
    }

    public static void setExportFileApocConfigs(boolean exportEnabled, boolean useNeo4jConfs, boolean allowReadFromFs) {
        apocConfig().setProperty(APOC_EXPORT_FILE_ENABLED, exportEnabled);
        setFileConfigsCommon(useNeo4jConfs, allowReadFromFs);
    }

    private static void setFileConfigsCommon(boolean useNeo4jConfs, boolean allowReadFromFs) {
        apocConfig().setProperty(APOC_IMPORT_FILE_USE_NEO4J_CONFIG, useNeo4jConfs);
        apocConfig().setProperty(APOC_IMPORT_FILE_ALLOW__READ__FROM__FILESYSTEM, allowReadFromFs);
    }
}
