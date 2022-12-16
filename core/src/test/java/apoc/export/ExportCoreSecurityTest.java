package apoc.export;

import apoc.ApocConfig;
import apoc.ApocSettings;
import apoc.export.csv.ExportCSV;
import apoc.export.cypher.ExportCypher;
import apoc.export.graphml.ExportGraphML;
import apoc.export.json.ExportJson;
import apoc.util.FileUtils;
import apoc.util.TestUtil;
import junit.framework.TestCase;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.QueryExecutionException;
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
    private static final File directoryWithSamePrefix = new File("target/imported");
    private static final File subDirectory = new File("target/import/tests");
    private static final List<String> APOC_EXPORT_PROCEDURE_NAME = Arrays.asList("csv", "json", "graphml", "cypher");

    static {
        //noinspection ResultOfMethodCallIgnored
        directory.mkdirs();
        //noinspection ResultOfMethodCallIgnored
        subDirectory.mkdirs();
        //noinspection ResultOfMethodCallIgnored
        directoryWithSamePrefix.mkdirs();
    }

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule()
            .withSetting(GraphDatabaseSettings.load_csv_file_url_root, directory.toPath().toAbsolutePath())
            .withSetting(ApocSettings.apoc_export_file_enabled, false);

    @BeforeClass
    public static void setUp() throws Exception {
        TestUtil.registerProcedure(db, ExportCSV.class, ExportJson.class, ExportGraphML.class, ExportCypher.class);
        setFileExport(false);
    }

    private static void setFileExport(boolean allowed) {
        ApocConfig.apocConfig().setProperty(ApocConfig.APOC_EXPORT_FILE_ENABLED, allowed);
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
            QueryExecutionException e = Assert.assertThrows(QueryExecutionException.class,
                    () -> TestUtil.testCall(db, "CALL " + apocProcedure, (r) -> {})
            );

            assertError(e, ApocConfig.EXPORT_TO_FILE_ERROR, RuntimeException.class, apocProcedure);
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
                setFileExport(true);
                db.executeTransactionally("CALL " + apocProcedure, Map.of(),
                        Result::resultAsString);
                fail(message);
            } catch (Exception e) {
                assertError(e, FileUtils.ACCESS_OUTSIDE_DIR_ERROR, IOException.class, apocProcedure);
            } finally {
                setFileExport(false);
            }
        }
    }

    @RunWith(Parameterized.class)
    public static class TestPathTraversalAccess {
        private final String apocProcedure;

        public TestPathTraversalAccess(String exportMethod, String exportMethodType, String exportMethodArguments) {
            this.apocProcedure = "apoc.export." + exportMethod + "." + exportMethodType + "(" + exportMethodArguments + ")";
        }

        private static final String case1 = "'file:///%2e%2e%2f%2f%2e%2e%2f%2f%2e%2e%2f%2f%2e%2e%2f%2fapoc/test.txt'";
        private static final String case2 = "'file:///%2e%2e%2f%2ftest.txt'";
        private static final String case3 = "'../test.txt'";
        private static final String case4 = "'tests/../../test.txt'";
        private static final String case5 = "'tests/..//..//test.txt'";

        private static final List<String> cases = Arrays.asList(case1, case2, case3, case4, case5);

        private static final Map<String, List<String>> METHOD_ARGUMENTS = Map.of(
                "query",  cases.stream().map(
                        filePath -> "\"RETURN 1\", " + filePath + ", {}"
                ).collect(Collectors.toList()),
                "all", cases.stream().map(
                        filePath -> filePath + ", {}"
                ).collect(Collectors.toList()),
                "data", cases.stream().map(
                        filePath -> "[], [], " + filePath + ", {}"
                ).collect(Collectors.toList()),
                "graph", cases.stream().map(
                        filePath -> "{nodes: [], relationships: []}, " + filePath + ", {}"
                ).collect(Collectors.toList())
        );

        @Parameterized.Parameters
        public static Collection<String[]> data() {
            return ExportCoreSecurityTest.data(METHOD_ARGUMENTS);
        }

        @Test
        public void testPathTraversal() {
            setFileExport(true);

            QueryExecutionException e = Assert.assertThrows(QueryExecutionException.class,
                    () -> TestUtil.testCall(db, "CALL " + apocProcedure, (r) -> {})
            );

            assertError(e, FileUtils.ACCESS_OUTSIDE_DIR_ERROR, IOException.class, apocProcedure);
            setFileExport(false);
        }
    }

    /**
     * All of these will resolve to a local path after normalization which will point to
     * a non-existing directory in our import folder: /apoc. Causing them to error that is
     * not found. They all attempt to exit the import folder back to the apoc folder:
     * Directory Layout: .../apoc/core/target/import
     */
    @RunWith(Parameterized.class)
    public static class TestPathTraversalIsNormalised {
        private final String apocProcedure;

        public TestPathTraversalIsNormalised(String exportMethod, String exportMethodType, String exportMethodArguments) {
            this.apocProcedure = "apoc.export." + exportMethod + "." + exportMethodType + "(" + exportMethodArguments + ")";
        }

        private static final String case1 = "'file://%2e%2e%2f%2e%2e%2f%2e%2e%2f%2e%2e%2f/apoc/test.txt'";
        private static final String case2 = "'file://../../../../apoc/test.txt'";
        private static final String case3 = "'file:///..//..//..//..//apoc//core//..//test.txt'";
        private static final String case4 = "'file:///..//..//..//..//apoc/test.txt'";
        private static final String case5 = "'file://" + directory.getAbsolutePath() + "//..//..//..//..//apoc/test.txt'";
        private static final String case6 = "'file:///%252e%252e%252f%252e%252e%252f%252e%252e%252f%252e%252e%252f/apoc/test.txt'";

        private static final List<String> cases = Arrays.asList(case1, case2, case3, case4, case5, case6);

        private static final Map<String, List<String>> METHOD_ARGUMENTS = Map.of(
                "query", cases.stream().map(
                        filePath -> "\"RETURN 1\", " + filePath + ", {}"
                ).collect(Collectors.toList()),
                "all", cases.stream().map(
                        filePath -> filePath + ", {}"
                ).collect(Collectors.toList()),
                "data", cases.stream().map(
                        filePath -> "[], [], " + filePath + ", {}"
                ).collect(Collectors.toList()),
                "graph", cases.stream().map(
                        filePath -> "{nodes: [], relationships: []}, " + filePath + ", {}"
                ).collect(Collectors.toList())
        );

        @Parameterized.Parameters
        public static Collection<String[]> data() {
            return ExportCoreSecurityTest.data(METHOD_ARGUMENTS);
        }

        @Test
        public void testPathTraversal() {
            setFileExport(true);

            QueryExecutionException e = Assert.assertThrows(QueryExecutionException.class,
                    () -> TestUtil.testCall(db, "CALL " + apocProcedure, (r) -> {})
            );

            TestCase.assertTrue(e.getMessage().contains("apoc/test.txt (No such file or directory)"));
            setFileExport(false);
        }
    }

    /**
     * These tests normalize the path to be within the import directory and make the file there.
     * Some attempt to exit the directory.
     * They result in a file name test.txt being created (and deleted after).
     */
    @RunWith(Parameterized.class)
    public static class TestPathTraversalIsNormalisedWithinDirectory {
        private final String apocProcedure;

        public TestPathTraversalIsNormalisedWithinDirectory(String exportMethod, String exportMethodType, String exportMethodArguments) {
            this.apocProcedure = "apoc.export." + exportMethod + "." + exportMethodType + "(" + exportMethodArguments + ")";
        }

        private static final String case1 = "'file:///..//..//..//..//apoc//..//..//..//..//test.txt'";
        private static final String case2 = "'file:///..//..//..//..//apoc//..//test.txt'";
        private static final String case3 = "'file:///../import/../import//..//test.txt'";
        private static final String case4 = "'file://test.txt'";
        private static final String case5 = "'file://tests/../test.txt'";
        private static final String case6 = "'file:///tests//..//test.txt'";
        private static final String case7 = "'test.txt'";
        private static final String case8 = "'file:///..//..//..//..//test.txt'";

        private static final List<String> cases = Arrays.asList(case1, case2, case3, case4, case5, case6, case7, case8);

        private static final Map<String, List<String>> METHOD_ARGUMENTS = Map.of(
                "query", cases.stream().map(
                        filePath -> "\"RETURN 1\", " + filePath + ", {}"
                ).collect(Collectors.toList()),
                "all", cases.stream().map(
                        filePath -> filePath + ", {}"
                ).collect(Collectors.toList()),
                "data", cases.stream().map(
                        filePath -> "[], [], " + filePath + ", {}"
                ).collect(Collectors.toList()),
                "graph", cases.stream().map(
                        filePath -> "{nodes: [], relationships: []}, " + filePath + ", {}"
                ).collect(Collectors.toList())
        );

        @Parameterized.Parameters
        public static Collection<String[]> data() {
            return ExportCoreSecurityTest.data(METHOD_ARGUMENTS);
        }

        @Test
        public void testPathTraversal() {
            setFileExport(true);

            TestUtil.testCall(db, "CALL " + apocProcedure,
                    (r) -> assertTrue(((String) r.get("file")).contains("test.txt"))
            );

            File f = new File(directory.getAbsolutePath() + "/test.txt");
            TestCase.assertTrue(f.exists());
            TestCase.assertTrue(f.delete());
        }
    }

    /*
     * These test cases attempt to access a directory with the same prefix as the import directory. This is design to
     * test "directoryName.startsWith" logic which is a common path traversal bug.
     *
     * All these tests should fail because they access a directory which isn't the configured directory
     */
    @RunWith(Parameterized.class)
    public static class TestPathTraversalIsWithSimilarDirectoryName {
        private final String apocProcedure;

        public TestPathTraversalIsWithSimilarDirectoryName(String exportMethod, String exportMethodType, String exportMethodArguments) {
            this.apocProcedure = "apoc.export." + exportMethod + "." + exportMethodType + "(" + exportMethodArguments + ")";
        }

        private static final String case1 = "'../imported/test.txt'";
        private static final String case2 = "'tests/../../imported/test.txt'";

        private static final List<String> cases = Arrays.asList(case1, case2);

        private static final Map<String, List<String>> METHOD_ARGUMENTS = Map.of(
                "query", cases.stream().map(
                        filePath -> "\"RETURN 1\", " + filePath + ", {}"
                ).collect(Collectors.toList()),
                "all", cases.stream().map(
                        filePath -> filePath + ", {}"
                ).collect(Collectors.toList()),
                "data", cases.stream().map(
                        filePath -> "[], [], " + filePath + ", {}"
                ).collect(Collectors.toList()),
                "graph", cases.stream().map(
                        filePath -> "{nodes: [], relationships: []}, " + filePath + ", {}"
                ).collect(Collectors.toList())
        );

        @Parameterized.Parameters
        public static Collection<String[]> data() {
            return ExportCoreSecurityTest.data(METHOD_ARGUMENTS);
        }

        @Test
        public void testPathTraversal() {
            setFileExport(true);

            QueryExecutionException e = Assert.assertThrows(QueryExecutionException.class,
                    () -> TestUtil.testCall(db, "CALL " + apocProcedure, (r) -> {})
            );

            assertError(e, FileUtils.ACCESS_OUTSIDE_DIR_ERROR, IOException.class, apocProcedure);

            setFileExport(false);
        }
    }


    /**
     * These tests normalize the path to be within the import directory and step into a subdirectory
     * to make the file there.
     * Some attempt to exit the directory.
     * They result in a file name test.txt in the directory /tests being created (and deleted after).
     */
    @RunWith(Parameterized.class)
    public static class TestPathTraversAllowedWithinDirectory {
        private final String apocProcedure;

        public TestPathTraversAllowedWithinDirectory(String exportMethod, String exportMethodType, String exportMethodArguments) {
            this.apocProcedure = "apoc.export." + exportMethod + "." + exportMethodType + "(" + exportMethodArguments + ")";
        }

        private static final String case1 = "'file:///../import/../import//..//tests/test.txt'";
        private static final String case2 = "'file:///..//..//..//..//apoc//..//tests/test.txt'";
        private static final String case3 = "'file:///../import/../import//..//tests/../tests/test.txt'";
        private static final String case4 = "'file:///tests/test.txt'";
        private static final String case5 = "'tests/test.txt'";

        private static final List<String> cases = Arrays.asList(case1, case2, case3, case4, case5);

        private static final Map<String, List<String>> METHOD_ARGUMENTS = Map.of(
                "query", cases.stream().map(
                        filePath -> "\"RETURN 1\", " + filePath + ", {}"
                ).collect(Collectors.toList()),
                "all", cases.stream().map(
                        filePath -> filePath + ", {}"
                ).collect(Collectors.toList()),
                "data", cases.stream().map(
                        filePath -> "[], [], " + filePath + ", {}"
                ).collect(Collectors.toList()),
                "graph", cases.stream().map(
                        filePath -> "{nodes: [], relationships: []}, " + filePath + ", {}"
                ).collect(Collectors.toList())
        );

        @Parameterized.Parameters
        public static Collection<String[]> data() {
            return ExportCoreSecurityTest.data(METHOD_ARGUMENTS);
        }

        @Test
        public void testPathTraversal() {
            setFileExport(true);

            TestUtil.testCall(db, "CALL " + apocProcedure,
                    (r) -> assertTrue(((String) r.get("file")).contains("tests/test.txt"))
            );

            File f = new File(subDirectory.getAbsolutePath() + "/test.txt");
            TestCase.assertTrue(f.exists());
            TestCase.assertTrue(f.delete());
        }
    }

    public static class TestCypherSchema {
        private final String apocProcedure = "apoc.export.cypher.schema(%s)";

        @Test
        public void testIllegalFSAccessExportCypherSchema() {
            QueryExecutionException e = Assert.assertThrows(QueryExecutionException.class,
                    () -> TestUtil.testCall(db, String.format("CALL " + apocProcedure, "'./hello', {}"), (r) -> {})
            );
            assertError(e, ApocConfig.EXPORT_TO_FILE_ERROR, RuntimeException.class, apocProcedure);
        }

        @Test
        public void testIllegalExternalFSAccessExportCypherSchema() {
            setFileExport(true);
            QueryExecutionException e = Assert.assertThrows(QueryExecutionException.class,
                    () -> TestUtil.testCall(db, String.format("CALL " + apocProcedure, "'../hello', {}"), (r) -> {})
            );
            assertError(e, FileUtils.ACCESS_OUTSIDE_DIR_ERROR, IOException.class, apocProcedure);
            setFileExport(false);
        }
    }

    private static void assertError(Exception e, String errorMessage, Class<? extends Exception> exceptionType, String apocProcedure) {
        final Throwable rootCause = ExceptionUtils.getRootCause(e);
        assertTrue(apocProcedure + " should throw an instance of " + exceptionType.getSimpleName(), exceptionType.isInstance(rootCause));
        assertEquals(apocProcedure + " should throw the following message", errorMessage, rootCause.getMessage());
    }

}