package apoc.export;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static apoc.export.SecurityTestUtil.getApocProcedure;
import static apoc.export.SecurityTestUtil.setExportFileApocConfigs;
import static org.junit.Assert.assertTrue;

/**
 * These tests normalize the path to be within the import directory (or subdirectory) and make the file there.
 * Some attempt to exit the directory.
 */
@RunWith( Parameterized.class)
public class TestPathTraversalIsNormalisedWithinDirectory extends ExportCoreSecurityTestHarness
{

    public static final Consumer<Map> MAIN_DIR_CONSUMER = ( r) -> assertTrue( ((String) r.get( "file")).contains( "" + FILENAME));
    public static final Consumer<Map> SUB_DIR_CONSUMER = (r) -> assertTrue(((String) r.get("file")).contains("tests/" + FILENAME));

    private final String apocProcedure;
    private final String fileName;
    private final Consumer consumer;

    public TestPathTraversalIsNormalisedWithinDirectory(String exportMethod, String exportMethodType, String exportMethodArguments,
                                                        String fileName,
                                                        Consumer consumer) {
        this.apocProcedure = getApocProcedure(exportMethod, exportMethodType, exportMethodArguments);
        this.fileName = fileName;
        this.consumer = consumer;
    }

    /*
     These tests normalize the path to be within the import directory and make the file there.
     They result in a file being created (and deleted after).
     */
    private static final String caseBase = "./" + FILENAME;

    private static final String case01 = "file:///..//..//..//..//apoc//..//..//..//..//" + FILENAME;
    private static final String case02 = "file:///..//..//..//..//apoc//..//" + FILENAME;
    private static final String case03 = "file:///../import/../import//..//" + FILENAME;
    private static final String case04 = "file://" + FILENAME;
    private static final String case05 = "file://tests/../" + FILENAME;
    private static final String case06 = "file:///tests//..//" + FILENAME;
    private static final String case07 = "" + FILENAME;
    private static final String case08 = "file:///..//..//..//..//" + FILENAME;

    public static final List<String> mainDirCases = Arrays.asList( caseBase, case01, case02, case03, case04, case05, case06, case07, case08);

    /*
     These tests normalize the path to be within the import directory and step into a subdirectory
     to make the file there.
     They result in a file in the directory /tests being created (and deleted after).
     */
    private static final String case11 = "file:///../import/../import//..//tests/" + FILENAME;
    private static final String case12 = "file:///..//..//..//..//apoc//..//tests/" + FILENAME;
    private static final String case13 = "file:///../import/../import//..//tests/../tests/" + FILENAME;
    private static final String case14 = "file:///tests/" + FILENAME;
    private static final String case15 = "tests/" + FILENAME;

    public static final List<String> subDirCases = Arrays.asList(case11, case12, case13, case14, case15);


    @Parameterized.Parameters(name = PARAM_NAMES)
    public static Collection<Object[]> data() {
        List<Pair<String, Consumer<Map>>> collect = mainDirCases.stream().map( i -> Pair.of( i, MAIN_DIR_CONSUMER)).collect( Collectors.toList());
        List<Pair<String, Consumer<Map>>> collect2 = subDirCases.stream().map(i -> Pair.of(i, SUB_DIR_CONSUMER)).collect(Collectors.toList());
        collect.addAll(collect2);

        return getParameterData(collect);
    }

    @Test
    public void testPathTraversal() {
        File dir = getDir();
        setExportFileApocConfigs(true, true, false);

        assertPathTraversalWithoutErrors(dir);
    }

    @Test
    public void testIllegalFSAccessExport() {
        SecurityTestUtil.testsWithExportDisabled(db, apocProcedure, fileName);
    }

    @Test
    public void testWithUseNeo4jConfDisabled() {
        // all assertions with `apoc.export.file.enabled=true` and `apoc.import.file.use_neo4j_config=true`

        File dir = getDir();

        // apoc.import.file.allow_read_from_filesystem=false
        setExportFileApocConfigs(true, true, false);
        assertPathTraversalWithoutErrors(dir);

        // apoc.import.file.allow_read_from_filesystem=true
        setExportFileApocConfigs(true, true, true);
        assertPathTraversalWithoutErrors(dir);
    }

    private File getDir() {
        if (subDirCases.contains(fileName)) {
            return subDirectory;
        }
        return directory;
    }

    private void assertPathTraversalWithoutErrors(File directory) {
        File file = new File(directory.getAbsolutePath(), FILENAME);
        SecurityTestUtil.assertPathTraversalWithoutErrors(db, apocProcedure, fileName, file);
    }

    // tests with `apoc.import.file.use_neo4j_config=false` not implemented because the results can be different
    // e.g. based on project folder name, so the exported file can be basically everywhere

}
