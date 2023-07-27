package apoc.export;

import apoc.util.FileUtils;
import apoc.util.Util;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.neo4j.graphdb.QueryExecutionException;

import static apoc.export.SecurityTestUtil.ERROR_KEY;
import static apoc.export.SecurityTestUtil.PROCEDURE_KEY;
import static apoc.export.SecurityTestUtil.assertPathTraversalError;
import static apoc.export.SecurityTestUtil.getApocProcedure;
import static apoc.export.SecurityTestUtil.setExportFileApocConfigs;
import static apoc.util.TestUtil.assertError;
import static org.junit.Assert.assertTrue;

/**
 * These test with `apoc.import.file.use_neo4j_config=true`
 * should fail because they access a directory which isn't the configured directory
 */
@RunWith( Parameterized.class)
public class TestIllegalExternalFSAccess extends ExportCoreSecurityTestHarness
{

    // these 2 consumers accept a Map.of("error", <errorMsg>, "procedure", <procedureQuery>)
    public static final Consumer<Map> EXCEPTION_OUTDIR_CONSUMER = ( Map e) -> assertError(
            (Exception) e.get(ERROR_KEY), FileUtils.ACCESS_OUTSIDE_DIR_ERROR, IOException.class, (String) e.get( PROCEDURE_KEY)
    );
    public static final Consumer<Map> EXCEPTION_NOT_FOUND_CONSUMER = (Map e) -> assertTrue(
            ((Exception) e.get(ERROR_KEY)).getMessage().contains("test.txt (No such file or directory)")
    );

    private final String apocProcedure;
    private final String fileName;
    private final Consumer consumer;

    public TestIllegalExternalFSAccess(String exportMethod, String exportMethodType, String exportMethodArguments,
                                       String fileName,
                                       Consumer consumer) {
        this.apocProcedure = getApocProcedure(exportMethod, exportMethodType, exportMethodArguments);
        this.fileName = fileName;
        this.consumer = consumer;
    }

    /*
     * These test cases attempt to access a directory with the same prefix as the import directory. This is design to
     * test "directoryName.startsWith" logic which is a common path traversal bug.
     * All these tests should fail because they access a directory which isn't the configured directory
     */
    private static final String case01 = "../imported/" + FILENAME;
    private static final String case02 = "tests/../../imported/" + FILENAME;
    private static final String case03 = "../" + FILENAME;
    private static final String case04 = "file:../" + FILENAME;
    private static final String case05 = "file:..//" + FILENAME;

    // non-failing cases, with apoc.import.file.use_neo4j_config=false
    public static final List<String> casesAllowed = Arrays.asList( case03, case04, case05);

    private static final String case06 = "file:///%2e%2e%2f%2f%2e%2e%2f%2f%2e%2e%2f%2f%2e%2e%2f%2fapoc/" + FILENAME;
    public static final String case07 = "file:///%2e%2e%2f%2f" + FILENAME;
    private static final String case08 = "tests/../../" + FILENAME;
    private static final String case09 = "tests/..//..//" + FILENAME;

    public static final List<String> casesOutsideDir = Arrays.asList(case01, case02, case03, case04, case05,
                                                                     case06, case07, case08, case09);

    /*
     All of these will resolve to a local path after normalization which will point to
     a non-existing directory in our import folder: /apoc. Causing them to error that is
     not found. They all attempt to exit the import folder back to the apoc folder:
     Directory Layout: .../apoc/core/target/import
     */
    private static final String case10 = "file://%2e%2e%2f%2e%2e%2f%2e%2e%2f%2e%2e%2f/apoc/" + FILENAME;
    private static final String case11 = "file://../../../../apoc/" + FILENAME;
    private static final String case12 = "file:///..//..//..//..//apoc//core//..//" + FILENAME;
    private static final String case13 = "file:///..//..//..//..//apoc/" + FILENAME;
    private static final String case14 = "file://" + directory.getAbsolutePath() + "//..//..//..//..//apoc/" + FILENAME;
    private static final String case15 = "file:///%252e%252e%252f%252e%252e%252f%252e%252e%252f%252e%252e%252f/apoc/" + FILENAME;

    public static final List<String> casesNotExistingDir = Arrays.asList(case10, case11, case12, case13, case14, case15);

    public static List<Pair<String, Consumer<Map>>> dataPairs;

    static {
        dataPairs = casesOutsideDir.stream()
                                   .map(i -> Pair.of(i, EXCEPTION_OUTDIR_CONSUMER))
                                   .collect( Collectors.toList());

        List<Pair<String, Consumer<Map>>> notExistingDirList = casesNotExistingDir.stream()
                                                                                  .map(i -> Pair.of(i, EXCEPTION_NOT_FOUND_CONSUMER))
                                                                                  .collect(Collectors.toList());

        dataPairs.addAll(notExistingDirList);
    }

    @Parameterized.Parameters(name = PARAM_NAMES)
    public static Collection<Object[]> data() {
        return getParameterData(dataPairs);
    }

    @Test
    public void testsWithExportDisabled() {
        SecurityTestUtil.testsWithExportDisabled(db, apocProcedure, fileName);
    }

    @Test
    public void testIllegalExternalFSAccessExportWithExportAndUseNeo4jConfEnabled() {
        // all assertions with `apoc.export.file.enabled=true` and `apoc.import.file.use_neo4j_config=true`

        // apoc.import.file.allow_read_from_filesystem=false
        setExportFileApocConfigs(true, true, false);
        assertPathTraversalError(db, apocProcedure, Map.of("fileName", fileName), consumer);

        // apoc.import.file.allow_read_from_filesystem=true
        setExportFileApocConfigs(true, true, true);
        assertPathTraversalError(db, apocProcedure, Map.of("fileName", fileName), consumer);
    }

    @Test
    public void testWithUseNeo4jConfDisabled() {
        // all assertions with `apoc.export.file.enabled=true` and `apoc.import.file.use_neo4j_config=false`

        // apoc.import.file.allow_read_from_filesystem=true
        setExportFileApocConfigs(true, false, true);
        testWithUseNeo4jConfFalse();

        // apoc.import.file.allow_read_from_filesystem=false
        setExportFileApocConfigs(true, false, false);
        testWithUseNeo4jConfFalse();
    }

    private void testWithUseNeo4jConfFalse() {
        // with `apoc.import.file.use_neo4j_config=false` this file export could outside the project
        if (fileName.equals(case07)) {
            return;
        }

        try {
            assertPathTraversalWithoutErrors();
        } catch ( QueryExecutionException e) {
            EXCEPTION_NOT_FOUND_CONSUMER.accept(
                    Util.map( ERROR_KEY, e, PROCEDURE_KEY, apocProcedure)
            );
        }
    }

    private void assertPathTraversalWithoutErrors() {
        SecurityTestUtil.assertPathTraversalWithoutErrors(db, apocProcedure, fileName, new File( "../", FILENAME));
    }
}
