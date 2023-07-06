package apoc.load;

import apoc.ApocConfig;
import apoc.util.FileUtils;
import apoc.util.SensitivePathGenerator;
import apoc.util.TestUtil;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.junit.AfterClass;
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
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static apoc.ApocConfig.APOC_IMPORT_FILE_ENABLED;
import static apoc.ApocConfig.apocConfig;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(Enclosed.class)
public class LoadExtendedSecurityTest {

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
            .withSetting(GraphDatabaseSettings.load_csv_file_url_root, import_folder);

    @BeforeClass
    public static void setUp() throws Exception {
        TestUtil.registerProcedure(db, LoadXls.class, LoadHtml.class, LoadCsv.class);
        apocConfig().setProperty(APOC_IMPORT_FILE_ENABLED, false);
    }

    @AfterClass
    public static void tearDown() {
        db.shutdown();
    }

    private static final Map<String, Class<?>> ALLOWED_EXCEPTIONS = Map.of(
            "xls", IOException.class);

    private static final Map<String, List<String>> APOC_PROCEDURE_WITH_ARGUMENTS = Map.of(
            "xls", List.of("($fileName, '', {})"),
            "html", List.of("($fileName)"),
            "csv", List.of("($fileName)"),
            "csvParams", List.of("($fileName, {}, '', {})"));

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
            return LoadExtendedSecurityTest.data();
        }

        @Test
        public void testIllegalFSAccessWithImportDisabled() {
            apocConfig().setProperty(ApocConfig.APOC_IMPORT_FILE_ENABLED, false);
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
            final String fileName = SensitivePathGenerator.etcPasswd().getLeft();
            apocConfig().setProperty(ApocConfig.APOC_IMPORT_FILE_ENABLED, true);
            apocConfig().setProperty(ApocConfig.APOC_IMPORT_FILE_USE_NEO4J_CONFIG, true);
            apocConfig().setProperty(ApocConfig.APOC_IMPORT_FILE_ALLOW__READ__FROM__FILESYSTEM, false);
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
            final String fileName = SensitivePathGenerator.etcPasswd().getLeft();
            apocConfig().setProperty(ApocConfig.APOC_IMPORT_FILE_ENABLED, true);
            apocConfig().setProperty(ApocConfig.APOC_IMPORT_FILE_USE_NEO4J_CONFIG, false);
            apocConfig().setProperty(ApocConfig.APOC_IMPORT_FILE_ALLOW__READ__FROM__FILESYSTEM, true);
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