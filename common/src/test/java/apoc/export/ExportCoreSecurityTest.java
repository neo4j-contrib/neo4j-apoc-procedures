package apoc.export;

import apoc.ApocConfig;
import apoc.ApocSettings;
import apoc.export.csv.ExportCSV;
import apoc.export.cypher.ExportCypher;
import apoc.export.graphml.ExportGraphML;
import apoc.export.json.ExportJson;
import apoc.util.FileUtils;
import apoc.util.TestUtil;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.Result;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

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
public class ExportCoreSecurityTest {

    private static final File directory = new File("target/import");
    private static final List<String> APOC_EXPORT_PROCEDURE_NAME = Arrays.asList("csv", "json", "graphml", "cypher");

    static {
        directory.mkdirs();
    }

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule()
            .withSetting(GraphDatabaseSettings.load_csv_file_url_root, directory.toPath().toAbsolutePath())
            .withSetting(ApocSettings.apoc_export_file_enabled, false);

    @BeforeClass
    public static void setUp() throws Exception {
        TestUtil.registerProcedure(db, ExportCSV.class, ExportJson.class, ExportGraphML.class, ExportCypher.class);
    }

    @Before
    public void before() {
        ApocConfig.apocConfig().setProperty(ApocSettings.apoc_export_file_enabled, false);
    }

    @After
    public void after() {
        ApocConfig.apocConfig().setProperty(ApocSettings.apoc_export_file_enabled, false);
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

    @RunWith(Parameterized.class)
    public static class TestIllegalFSAccess {
        private final String apocProcedure;

        public TestIllegalFSAccess(String exportMethod, String exportMethodType, String exportMethodArguments) {
            this.apocProcedure = "apoc.export." + exportMethod + "." + exportMethodType + "(" + exportMethodArguments + ")";
        }

        private static final Map<String, List<String>> APOC_PROCEDURE_ARGUMENTS = Map.of(
                "query", List.of(
                        "\"RETURN 'hello' as key\", './hello', {}",
                        "\"RETURN 'hello' as key\", './hello', {stream:true}",
                        "\"RETURN 'hello' as key\", '  ', {}"
                ),
                "all", List.of(
                        "'./hello', {}",
                        "'./hello', {stream:true}",
                        "'  ', {}"
                ),
                "data", List.of(
                        "[], [], './hello', {}",
                        "[], [], './hello', {stream:true}",
                        "[], [], '  ', {}"
                ),
                "graph", List.of(
                        "{nodes: [], relationships: []}, './hello', {}",
                        "{nodes: [], relationships: []}, './hello', {stream:true}",
                        "{nodes: [], relationships: []}, '  ', {}"
                )
        );

        @Parameterized.Parameters
        public static Collection<String[]> data() {
            return ExportCoreSecurityTest.data(APOC_PROCEDURE_ARGUMENTS);
        }

        @Test
        public void testIllegalFSAccessExport() {
            final String message = apocProcedure + " should throw an exception";
            try {
                db.executeTransactionally("CALL " + apocProcedure, Map.of(),
                        Result::resultAsString);
                fail(message);
            } catch (Exception e) {
                assertError(e, ApocConfig.EXPORT_TO_FILE_ERROR, RuntimeException.class, apocProcedure);
            }
        }

    }

    @RunWith(Parameterized.class)
    public static class TestIllegalExternalFSAccess {
        private final String apocProcedure;

        public TestIllegalExternalFSAccess(String exportMethod, String exportMethodType, String exportMethodArguments) {
            this.apocProcedure = "apoc.export." + exportMethod + "." + exportMethodType + "(" + exportMethodArguments + ")";
        }

        private static final Map<String, List<String>> METHOD_ARGUMENTS = Map.of(
                "query", List.of(
                        "\"RETURN 'hello' as key\", '../hello', {}",
                        "\"RETURN 'hello' as key\", 'file:../hello', {}"
                ),
                "all", List.of(
                        "'../hello', {}",
                        "'file:../hello', {}"
                ),
                "data", List.of(
                        "[], [], '../hello', {}",
                        "[], [], 'file:../hello', {}"
                ),
                "graph", List.of(
                        "{nodes: [], relationships: []}, '../hello', {}",
                        "{nodes: [], relationships: []}, 'file:../hello', {}"
                )
        );

        @Parameterized.Parameters
        public static Collection<String[]> data() {
            return ExportCoreSecurityTest.data(METHOD_ARGUMENTS);
        }

        @Test
        public void testIllegalExternalFSAccessExport() {
            final String message = apocProcedure + " should throw an exception";
            try {
                ApocConfig.apocConfig().setProperty(ApocSettings.apoc_export_file_enabled, true);
                db.executeTransactionally("CALL " + apocProcedure, Map.of(),
                        Result::resultAsString);
                fail(message);
            } catch (Exception e) {
                assertError(e, FileUtils.ACCESS_OUTSIDE_DIR_ERROR, IOException.class, apocProcedure);
            } finally {
                ApocConfig.apocConfig().setProperty(ApocSettings.apoc_export_file_enabled, false);
            }
        }
    }

    public static class TestCypherSchema {
        private final String apocProcedure = "apoc.export.cypher.schema(%s)";
        private final String message = apocProcedure + " should throw an exception";

        @Test
        public void testIllegalFSAccessExportCypherSchema() {
            try {
                db.executeTransactionally(String.format("CALL " + apocProcedure, "'./hello', {}"), Map.of(),
                        Result::resultAsString);
                fail(message);
            } catch (Exception e) {
                assertError(e, ApocConfig.EXPORT_TO_FILE_ERROR, RuntimeException.class, apocProcedure);
            }
        }

        @Test
        public void testIllegalExternalFSAccessExportCypherSchema() {
            try {
                ApocConfig.apocConfig().setProperty(ApocSettings.apoc_export_file_enabled, true);
                db.executeTransactionally(String.format("CALL " + apocProcedure, "'../hello', {}"), Map.of(),
                        Result::resultAsString);
                fail(message);
            } catch (Exception e) {
                assertError(e, FileUtils.ACCESS_OUTSIDE_DIR_ERROR, IOException.class, apocProcedure);
            } finally {
                ApocConfig.apocConfig().setProperty(ApocSettings.apoc_export_file_enabled, false);
            }
        }
    }

    private static void assertError(Exception e, String errorMessage, Class<? extends Exception> exceptionType, String apocProcedure) {
        final Throwable rootCause = ExceptionUtils.getRootCause(e);
        assertTrue(apocProcedure + " should throw an instance of " + exceptionType.getSimpleName(), exceptionType.isInstance(rootCause));
        assertEquals(apocProcedure + " should throw the following message", errorMessage, rootCause.getMessage());
    }

}