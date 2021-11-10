package apoc.load;

import apoc.ApocConfig;
import apoc.ApocSettings;
import apoc.util.FileUtils;
import apoc.util.SensitivePathGenerator;
import apoc.util.TestUtil;
import com.fasterxml.jackson.core.JsonParseException;
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
import org.xml.sax.SAXParseException;

import java.io.File;
import java.net.MalformedURLException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(Enclosed.class)
public class LoadCoreSecurityTest {

    public static Path import_folder;

    static {
        try {
            import_folder = File.createTempFile(UUID.randomUUID().toString(), "tmp")
                    .getParentFile().toPath();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule()
            .withSetting(GraphDatabaseSettings.load_csv_file_url_root, import_folder)
            .withSetting(ApocSettings.apoc_import_file_enabled, false);

    @BeforeClass
    public static void setUp() throws Exception {
        TestUtil.registerProcedure(db, LoadJson.class, Xml.class);
    }

    @Before
    public void before() {
        ApocConfig.apocConfig().setProperty(ApocSettings.apoc_import_file_enabled, false);
    }

    @After
    public void after() {
        ApocConfig.apocConfig().setProperty(ApocSettings.apoc_import_file_enabled, false);
    }

    private static final Map<String, List<String>> APOC_PROCEDURE_WITH_ARGUMENTS = Map.of(
            "json", List.of("($fileName, '', {})"),
            "jsonArray", List.of("($fileName, '', {})"),
            "jsonParams", List.of("($fileName, {}, '')"),
            "xml", List.of("($fileName, '', {}, false)"),
            "xmlSimple", List.of("($fileName)"));

    private static final Map<String, Class<?>> ALLOWED_EXCEPTIONS = Map.of(
            "json", JsonParseException.class,
            "jsonArray", JsonParseException.class,
            "jsonParams", JsonParseException.class,
            "xml", SAXParseException.class,
            "xmlSimple", MalformedURLException.class);


    private static Collection<String[]> data() {
        return APOC_PROCEDURE_WITH_ARGUMENTS.entrySet()
                .stream()
                .flatMap(e -> e.getValue().stream().map(arg -> new String[]{e.getKey(), arg}))
                .collect(Collectors.toList());
    }

    @RunWith(Parameterized.class)
    public static class TestIllegalFSAccess {
        private final String apocProcedure;
        private final String exportMethod;

        public TestIllegalFSAccess(String exportMethod, String exportMethodArguments) {
            this.apocProcedure = "apoc.load." + exportMethod + exportMethodArguments;
            this.exportMethod = exportMethod;
        }

        @Parameterized.Parameters
        public static Collection<String[]> data() {
            return LoadCoreSecurityTest.data();
        }

        @Test
        public void testIllegalFSAccessWithImportDisabled() {
            ApocConfig.apocConfig().setProperty(ApocConfig.APOC_IMPORT_FILE_ENABLED, false);
            final String message = apocProcedure + " should throw an exception";
            try {
                db.executeTransactionally("CALL " + apocProcedure,
                        Map.of("fileName", "./hello"),
                        Result::resultAsString);
                fail(message);
            } catch (Exception e) {
                assertError(e, ApocConfig.LOAD_FROM_FILE_ERROR, RuntimeException.class, apocProcedure);
            }
        }

        @Test
        public void testIllegalFSAccessWithImportEnabled() {
            final String message = apocProcedure + " should throw an exception";
            final String fileName = SensitivePathGenerator.etcPasswd(db).first();
            ApocConfig.apocConfig().setProperty(ApocConfig.APOC_IMPORT_FILE_ENABLED, true);
            ApocConfig.apocConfig().setProperty(ApocConfig.APOC_IMPORT_FILE_USE_NEO4J_CONFIG, true);
            ApocConfig.apocConfig().setProperty(ApocConfig.APOC_IMPORT_FILE_ALLOW__READ__FROM__FILESYSTEM, false);
            try {
                db.executeTransactionally("CALL " + apocProcedure,
                        Map.of("fileName", fileName),
                        Result::resultAsString);
                fail(message);
            } catch (Exception e) {
                assertError(e, String.format(FileUtils.ERROR_READ_FROM_FS_NOT_ALLOWED, fileName), RuntimeException.class, apocProcedure);
            }
        }

        @Test
        public void testReadSensitiveFileWorks() {
            // as we're defining ApocConfig.APOC_IMPORT_FILE_ALLOW__READ__FROM__FILESYSTEM to true
            // and ApocConfig.APOC_IMPORT_FILE_ALLOW__READ__FROM__FILESYSTEM to false the next call should work
            final String fileName = SensitivePathGenerator.etcPasswd(db).first();
            ApocConfig.apocConfig().setProperty(ApocConfig.APOC_IMPORT_FILE_ENABLED, true);
            ApocConfig.apocConfig().setProperty(ApocConfig.APOC_IMPORT_FILE_USE_NEO4J_CONFIG, false);
            ApocConfig.apocConfig().setProperty(ApocConfig.APOC_IMPORT_FILE_ALLOW__READ__FROM__FILESYSTEM, true);
            try {
                db.executeTransactionally("CALL " + apocProcedure,
                        Map.of("fileName", fileName),
                        Result::resultAsString);
                if (ALLOWED_EXCEPTIONS.containsKey(exportMethod)) {
                    fail("Expected to fail by throwing the following exception: " + ALLOWED_EXCEPTIONS.get(exportMethod));
                }
            } catch (Exception e) {
                if (ALLOWED_EXCEPTIONS.containsKey(exportMethod)) {
                    assertEquals(ALLOWED_EXCEPTIONS.get(exportMethod), ExceptionUtils.getRootCause(e).getClass());
                }
            }
        }
    }

    private static void assertError(Exception e, String errorMessage, Class<? extends Exception> exceptionType, String apocProcedure) {
        final Throwable rootCause = ExceptionUtils.getRootCause(e);
        assertTrue(apocProcedure + " should throw an instance of " + exceptionType.getSimpleName(), exceptionType.isInstance(rootCause));
        assertEquals(apocProcedure + " should throw the following message", errorMessage, rootCause.getMessage());
    }

}
