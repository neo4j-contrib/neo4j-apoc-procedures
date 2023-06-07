package apoc.export;

import apoc.ApocConfig;
import apoc.export.csv.ImportCsv;
import apoc.export.graphml.ExportGraphML;
import apoc.export.json.ImportJson;
import apoc.load.LoadArrow;
import apoc.load.LoadJson;
import apoc.load.Xml;
import apoc.util.SensitivePathGenerator;
import apoc.util.TestUtil;
import com.nimbusds.jose.util.Pair;
import inet.ipaddr.IPAddressString;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.assertj.core.api.Assertions;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Result;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static apoc.export.SecurityTestUtil.ALLOWED_EXCEPTIONS;
import static apoc.export.SecurityTestUtil.IMPORT_PROCEDURES;
import static apoc.export.SecurityTestUtil.LOAD_PROCEDURES;
import static apoc.export.SecurityTestUtil.setImportFileApocConfigs;
import static apoc.util.FileTestUtil.createTempFolder;
import static apoc.util.FileUtils.ACCESS_OUTSIDE_DIR_ERROR;
import static apoc.util.FileUtils.ERROR_READ_FROM_FS_NOT_ALLOWED;
import static apoc.util.TestUtil.testCall;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
public class ImportAndLoadCoreSecurityTest {
    private static final Path TEMP_FOLDER = createTempFolder();

    // base path: "../../../../etc/passwd"
    private static final String ABSOLUTE_URL = SensitivePathGenerator.etcPasswd().getLeft();
    private static final String ABSOLUTE_URL_WITH_FILE_PREFIX = "file:" + ABSOLUTE_URL;
    private static final String ABSOLUTE_URL_WITH_FILE_SLASH = "file:/" + ABSOLUTE_URL;
    private static final String ABSOLUTE_URL_WITH_FILE_DOUBLE_SLASH = "file://" + ABSOLUTE_URL;
    private static final String ABSOLUTE_URL_WITH_FILE_TRIPLE_SLASH = "file:///" + ABSOLUTE_URL;

    // base path: "/etc/passwd"
    private static final String RELATIVE_URL = SensitivePathGenerator.etcPasswd().getRight();
    private static final String RELATIVE_URL_WITH_FILE_SLASH = "file:" + RELATIVE_URL;
    private static final String RELATIVE_URL_WITH_FILE_DOUBLE_SLASH = "file:/" + RELATIVE_URL;
    private static final String RELATIVE_URL_WITH_FILE_TRIPLE_SLASH = "file://" + RELATIVE_URL;


    private final String apocProcedure;
    private final String importMethod;
    private final String fileName;

    public ImportAndLoadCoreSecurityTest(String method, String methodArguments, String fileName) {
        this.apocProcedure = "CALL " + method + methodArguments;
        this.importMethod = method;
        this.fileName = fileName;
    }

    @Parameterized.Parameters(name = "Procedure: {0}{1}, fileName: {2}")
    public static Collection<String[]> data() {
        // transform `Pair(<KEY>, <VALUE>)` to `Pair(apoc.import.<KEY> , <VALUE>)`
        List<Pair<String, String>> importAndLoadProcedures = IMPORT_PROCEDURES
                .map(e -> Pair.of( "apoc.import." + e.getLeft() , e.getRight() ))
                .collect(Collectors.toList());

        // transform `Pair(<KEY>, <VALUE>)` to `Pair(apoc.load.<KEY> , <VALUE>)`
        List<Pair<String, String>> loadProcedures = LOAD_PROCEDURES
                .map(e -> Pair.of( "apoc.load." + e.getLeft() , e.getRight() ))
                .toList();

        importAndLoadProcedures.addAll(loadProcedures);

        return getParameterData(importAndLoadProcedures);
    }

    private static List<String[]> getParameterData(List<Pair<String, String>> importAndLoadProcedures) {
        // from a stream of fileNames and a List of Pair<procName, procArgs>
        // returns a List of String[]{ procName, procArgs, fileName }

        Stream<String> fileNames = Stream.of(ABSOLUTE_URL,
                ABSOLUTE_URL_WITH_FILE_PREFIX,
                ABSOLUTE_URL_WITH_FILE_SLASH,
                ABSOLUTE_URL_WITH_FILE_DOUBLE_SLASH,
                ABSOLUTE_URL_WITH_FILE_TRIPLE_SLASH,
                RELATIVE_URL,
                RELATIVE_URL_WITH_FILE_SLASH,
                RELATIVE_URL_WITH_FILE_DOUBLE_SLASH,
                RELATIVE_URL_WITH_FILE_TRIPLE_SLASH);

        return fileNames.flatMap(fileName -> importAndLoadProcedures
                        .stream()
                        .map(
                            procPair -> new String[] { procPair.getLeft(), procPair.getRight(), fileName }
                        )
                ).toList();
    }

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule()
            .withSetting(GraphDatabaseSettings.load_csv_file_url_root, TEMP_FOLDER)
            .withSetting(GraphDatabaseInternalSettings.cypher_ip_blocklist, List.of(new IPAddressString("127.168.0.0/8")));


    @BeforeClass
    public static void setUp() throws IOException {
        Logger logger = Logger.getLogger(ImportAndLoadCoreSecurityTest.class.getName());
        logger.setLevel(Level.SEVERE);

        TestUtil.registerProcedure(db,
                // import procedures (ExportGraphML contains the `apoc.import.graphml` too)
                ImportJson.class, Xml.class, ImportCsv.class, ExportGraphML.class,
                // load procedures (Xml contains both `apoc.load.xml` and `apoc.import.xml` procedures)
                LoadJson.class, LoadArrow.class);
    }
    

    @Test
    public void testIllegalFSAccessWithDifferentApocConfs() {
        // apoc.import.file.enabled=true
        // apoc.import.file.use_neo4j_config=true
        // apoc.import.file.allow_read_from_filesystem=true
        setImportFileApocConfigs(true, true, true);
        assertIpAddressBlocked();

        // apoc.import.file.enabled=true
        // apoc.import.file.use_neo4j_config=false
        // apoc.import.file.allow_read_from_filesystem=false
        setImportFileApocConfigs(true, false, false);
        assertIpAddressBlocked();

        // apoc.import.file.enabled=true
        // apoc.import.file.use_neo4j_config=true
        // apoc.import.file.allow_read_from_filesystem=false
        setImportFileApocConfigs(true, true, false);
        assertIpAddressBlocked();

        // apoc.import.file.enabled=true
        // apoc.import.file.use_neo4j_config=true
        // apoc.import.file.allow_read_from_filesystem=false
        setImportFileApocConfigs(true, false, true);
        assertIpAddressBlocked();

        // apoc.import.file.enabled=false
        // apoc.import.file.use_neo4j_config=true
        // apoc.import.file.allow_read_from_filesystem=false
        setImportFileApocConfigs(false, true, false);
        assertIpAddressBlocked();

        // apoc.import.file.enabled=false
        // apoc.import.file.use_neo4j_config=true
        // apoc.import.file.allow_read_from_filesystem=true
        setImportFileApocConfigs(false, true, true);
        assertIpAddressBlocked();

        // apoc.import.file.enabled=false
        // apoc.import.file.use_neo4j_config=false
        // apoc.import.file.allow_read_from_filesystem=false
        setImportFileApocConfigs(false, false, true);
        assertIpAddressBlocked();

        // apoc.import.file.enabled=false
        // apoc.import.file.use_neo4j_config=false
        // apoc.import.file.allow_read_from_filesystem=false
        setImportFileApocConfigs(false, false, false);
        assertIpAddressBlocked();
    }

    @Test
    public void testImportFileDisabled() {
        // all assertions with `apoc.import.file.enabled=false`

        // apoc.import.file.use_neo4j_config=false
        // apoc.import.file.allow_read_from_filesystem=false
        setImportFileApocConfigs(false, false, false);
        assertImportDisabled();

        // apoc.import.file.use_neo4j_config=true
        // apoc.import.file.allow_read_from_filesystem=false
        setImportFileApocConfigs(false, true, false);
        assertImportDisabled();

        // apoc.import.file.use_neo4j_config=false
        // apoc.import.file.allow_read_from_filesystem=true
        setImportFileApocConfigs(false, false, true);
        assertImportDisabled();

        // apoc.import.file.use_neo4j_config=true
        // apoc.import.file.allow_read_from_filesystem=true
        setImportFileApocConfigs(false, true, true);
        assertImportDisabled();
    }

    @Test
    public void testIllegalFSAccessWithImportAndUseNeo4jConfsEnabled() {
        // apoc.import.file.enabled=true
        // apoc.import.file.use_neo4j_config=true
        // apoc.import.file.allow_read_from_filesystem=false
        setImportFileApocConfigs(true, true, false);
        assertReadFromFsNotAllowed();
    }

    @Test
    public void testImportOutsideDirNotAllowedWithAllApocFileConfigsEnabled() {
        // apoc.import.file.enabled=true
        // apoc.import.file.use_neo4j_config=true
        // apoc.import.file.allow_read_from_filesystem=true
        setImportFileApocConfigs(true, true, true);

        // only `../../../etc/passwd` throw the error, other urls just don't find the file,
        // i.e.: `file:/../../../etc/passwd`, `file://../../../etc/passwd` and `file:///../../../etc/passwd`
        // and relative ones (like `/etc/passwd`)
        if (fileName.equals(ABSOLUTE_URL) || fileName.equals(ABSOLUTE_URL_WITH_FILE_PREFIX)) {
            assertImportOutsideDirNotAllowed();
        } else {
            assertFileNotExists();
        }
    }

    @Test
    public void testReadSensitiveFileWorksWithApocUseNeo4jConfigDisabled() {
        // all checks with `apoc.import.file.use_neo4j_config=false`

        // apoc.import.file.enabled=true
        // apoc.import.file.allow_read_from_filesystem=true
        setImportFileApocConfigs(true, false, true);
        shouldRead();

        // apoc.import.file.enabled=false
        // apoc.import.file.allow_read_from_filesystem=false
        setImportFileApocConfigs(true, false, false);
        shouldRead();
    }

    private void assertIpAddressBlocked() {
        Stream.of("https", "http", "ftp").forEach(protocol -> {

            String url = String.format("%s://127.168.0.0/test.file", protocol);
            QueryExecutionException e = assertThrows(QueryExecutionException.class,
                    () -> testCall(db,
                            apocProcedure,
                            Map.of("fileName", url),
                            (r) -> {}
                    )
            );
            assertTrue(e.getMessage().contains("access to /127.168.0.0 is blocked via the configuration property internal.dbms.cypher_ip_blocklist"));
        });
    }

    private void assertImportDisabled() {
        assertFailingProcedure(ApocConfig.LOAD_FROM_FILE_ERROR, RuntimeException.class);
    }

    private void assertReadFromFsNotAllowed() {
        assertFailingProcedure(String.format(ERROR_READ_FROM_FS_NOT_ALLOWED, fileName), RuntimeException.class);
    }

    private void assertFailingProcedure(String expectedError, Class exceptionClass) {
        final String message = apocProcedure + " should throw an exception";

        try {
            db.executeTransactionally(apocProcedure,
                    Map.of("fileName", fileName),
                    Result::resultAsString);
            fail(message);
        } catch (Exception e) {
            TestUtil.assertError(e, expectedError, exceptionClass, apocProcedure);
        }
    }

    private void shouldRead() {
        // the `file://../../../etc/passwd` is not found unlike the other similar urls,
        // i.e.: `file:/../../../etc/passwd`, `file:///../../../etc/passwd` and `../../../etc/passwd`
        if (fileName.equals(RELATIVE_URL_WITH_FILE_DOUBLE_SLASH)) {
            assertFileNotExists();
            return;
        }
        try {
            db.executeTransactionally(apocProcedure,
                    Map.of("fileName", fileName),
                    Result::resultAsString);
        } catch (Exception e) {
            if (ALLOWED_EXCEPTIONS.containsKey(importMethod)) {
                Class<?> rootCause = ExceptionUtils.getRootCause(e).getClass();
                Class<?> classException = ALLOWED_EXCEPTIONS.get(importMethod);
                assertTrue("The procedure throws an exception with class " + rootCause + " instead of " + classException,
                        classException.isAssignableFrom(rootCause)
                );
            }
        }
    }

    private void assertImportOutsideDirNotAllowed() {
        assertFailingProcedure(ACCESS_OUTSIDE_DIR_ERROR, IOException.class);
    }

    private void assertFileNotExists() {
        try {
            db.executeTransactionally(apocProcedure,
                    Map.of("fileName", fileName),
                    Result::resultAsString);
        } catch (Exception e) {
            final Throwable rootCause = ExceptionUtils.getRootCause(e);
            assertTrue(rootCause instanceof IOException);
            String message = e.getMessage();
            Assertions.assertThat(message).contains("Cannot open file ");
            Assertions.assertThat(message).contains(" for reading.");
        }
    }

}
