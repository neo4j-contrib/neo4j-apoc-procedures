package apoc.export;

import apoc.export.csv.ExportCSV;
import apoc.export.cypher.ExportCypher;
import apoc.export.graphml.ExportGraphML;
import apoc.export.json.ExportJson;
import apoc.export.xls.ExportXls;
import apoc.util.FileUtils;
import apoc.util.TestUtil;
import apoc.util.Util;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(Enclosed.class)
public class ExportSecurityTest {

    private static final File directory = new File("target/import");
    private static final List<String> APOC_EXPORT_PROCEDURE_NAME = Arrays.asList("csv", "json", "graphml", "cypher", "xls");

    static {
        directory.mkdirs();
    }

    private static Collection<String[]> data(Map<String, List<String>> apocProcedureArguments) {
        return APOC_EXPORT_PROCEDURE_NAME
                .stream()
                .flatMap(method -> apocProcedureArguments
                        .entrySet()
                        .stream()
                        .flatMap(e -> e.getValue()
                                .stream()
                                .map(a -> new String[]{method, e.getKey(), a})))
                .collect(Collectors.toList());
    }

    public static abstract class CommonFsAccess {
        @Rule
        public TestName testName = new TestName();

        GraphDatabaseService db;

        @Before
        public void setUp() throws Exception {
            db = new TestGraphDatabaseFactory()
                    .newImpermanentDatabaseBuilder()
                    .setConfig(GraphDatabaseSettings.load_csv_file_url_root, directory.toPath().toAbsolutePath().toString())
                    .setConfig("apoc.export.file.enabled", String.valueOf(testName.getMethodName().contains("WithExportEnabled")))
                    .newGraphDatabase();
            TestUtil.registerProcedure(db, ExportCSV.class, ExportJson.class, ExportGraphML.class, ExportCypher.class, ExportXls.class);
        }

        @After
        public void shutdown() {
            if (db != null) {
                db.shutdown();
            }
        }
    }

    @RunWith(Parameterized.class)
    public static class TestIllegalFSAccess extends CommonFsAccess {
        private final String apocProcedure;

        public TestIllegalFSAccess(String exportMethod, String exportMethodType, String exportMethodArguments) {
            this.apocProcedure = "apoc.export." + exportMethod + "." + exportMethodType + "(" + exportMethodArguments + ")";
        }

        private static final Map<String, List<String>> APOC_PROCEDURE_ARGUMENTS = Util.map(
                "query", Arrays.asList(
                        "\"RETURN 'hello' as key\", './hello', {}",
                        "\"RETURN 'hello' as key\", './hello', {stream:true}",
                        "\"RETURN 'hello' as key\", '  ', {}"
                ),
                "all", Arrays.asList(
                        "'./hello', {}",
                        "'./hello', {stream:true}",
                        "'  ', {}"
                ),
                "data", Arrays.asList(
                        "[], [], './hello', {}",
                        "[], [], './hello', {stream:true}",
                        "[], [], '  ', {}"
                ),
                "graph", Arrays.asList(
                        "{nodes: [], relationships: []}, './hello', {}",
                        "{nodes: [], relationships: []}, './hello', {stream:true}",
                        "{nodes: [], relationships: []}, '  ', {}"
                )
        );

        @Parameterized.Parameters
        public static Collection<String[]> data() {
            return ExportSecurityTest.data(APOC_PROCEDURE_ARGUMENTS);
        }

        @Test
        public void testIllegalFSAccessExport() {
            final String message = apocProcedure + " should throw an exception";
            try {
                db.execute("CALL " + apocProcedure, Util.map()).resultAsString();
                fail(message);
            } catch (Exception e) {
                assertError(e, FileUtils.EXPORT_TO_FILE_ERROR, RuntimeException.class, apocProcedure);
            }
        }

    }

    @RunWith(Parameterized.class)
    public static class TestIllegalExternalFSAccess extends CommonFsAccess {
        private final String apocProcedure;

        public TestIllegalExternalFSAccess(String exportMethod, String exportMethodType, String exportMethodArguments) {
            this.apocProcedure = "apoc.export." + exportMethod + "." + exportMethodType + "(" + exportMethodArguments + ")";
        }

        private static final Map<String, List<String>> METHOD_ARGUMENTS = Util.map(
                "query", Arrays.asList(
                        "\"RETURN 'hello' as key\", '../hello', {}",
                        "\"RETURN 'hello' as key\", 'file:../hello', {}"
                ),
                "all", Arrays.asList(
                        "'../hello', {}",
                        "'file:../hello', {}"
                ),
                "data", Arrays.asList(
                        "[], [], '../hello', {}",
                        "[], [], 'file:../hello', {}"
                ),
                "graph", Arrays.asList(
                        "{nodes: [], relationships: []}, '../hello', {}",
                        "{nodes: [], relationships: []}, 'file:../hello', {}"
                )
        );

        @Parameterized.Parameters
        public static Collection<String[]> data() {
            return ExportSecurityTest.data(METHOD_ARGUMENTS);
        }

        @Test
        public void testIllegalExternalFSAccessExportWithExportEnabled() {
            final String message = apocProcedure + " should throw an exception";
            try {
                db.execute("CALL " + apocProcedure, Util.map()).resultAsString();
                fail(message);
            } catch (Exception e) {
                assertError(e, FileUtils.ACCESS_OUTSIDE_DIR_ERROR, IOException.class, apocProcedure);
            }
        }
    }

    public static class TestCypherSchema extends CommonFsAccess {
        private final String apocProcedure = "apoc.export.cypher.schema(%s)";
        private final String message = apocProcedure + " should throw an exception";

        @Test
        public void testIllegalFSAccessExportCypherSchema() {
            try {
                db.execute(String.format("CALL " + apocProcedure, "'./hello', {}"), Util.map()).resultAsString();
                fail(message);
            } catch (Exception e) {
                assertError(e, FileUtils.EXPORT_TO_FILE_ERROR, RuntimeException.class, apocProcedure);
            }
        }

        @Test
        public void testIllegalExternalFSAccessExportCypherSchemaWithExportEnabled() {
            try {
                db.execute(String.format("CALL " + apocProcedure, "'../hello', {}"), Util.map()).resultAsString();
                fail(message);
            } catch (Exception e) {
                assertError(e, FileUtils.ACCESS_OUTSIDE_DIR_ERROR, IOException.class, apocProcedure);
            }
        }
    }

    private static void assertError(Exception e, String errorMessage, Class<? extends Exception> exceptionType, String apocProcedure) {
        final Throwable rootCause = ExceptionUtils.getRootCause(e);
        assertEquals(apocProcedure + " should throw the following message", errorMessage, rootCause.getMessage());
        assertTrue(apocProcedure + " should throw an instance of " + exceptionType.getSimpleName(), exceptionType.isInstance(rootCause));
    }

}