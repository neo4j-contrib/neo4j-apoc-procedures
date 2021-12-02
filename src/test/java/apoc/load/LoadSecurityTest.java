package apoc.load;

import apoc.util.FileUtils;
import apoc.util.SensitivePathGenerator;
import apoc.util.TestUtil;
import apoc.util.Util;
import com.fasterxml.jackson.core.JsonParseException;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.xml.sax.SAXParseException;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(Enclosed.class)
public class LoadSecurityTest {

    public static Path import_folder;

    static {
        try {
            import_folder = File.createTempFile(UUID.randomUUID().toString(), "tmp")
                    .getParentFile().toPath();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static final Map<String, List<String>> APOC_PROCEDURE_WITH_ARGUMENTS = Util.map(
            "json", Arrays.asList("($fileName, '', {})"),
            "jsonArray", Arrays.asList("($fileName, '')"),
            "jsonParams", Arrays.asList("($fileName, {}, '')"),
            "xml", Arrays.asList("($fileName, '', {}, false)"),
            "xmlSimple", Arrays.asList("($fileName)"),
            "xls", Arrays.asList("($fileName, '', {})"),
            "html", Arrays.asList("($fileName)"),
            "csv", Arrays.asList("($fileName)"));

    private static final Map<String, Class<?>> ALLOWED_EXCEPTIONS = Util.map(
            "json", JsonParseException.class,
            "jsonArray", JsonParseException.class,
            "jsonParams", JsonParseException.class,
            "xml", SAXParseException.class,
            "xmlSimple", MalformedURLException.class,
            "xls", IOException.class);


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

        @Rule
        public TestName testName = new TestName();

        private GraphDatabaseAPI db;

        @Before
        public void before() throws Exception {
            db = (GraphDatabaseAPI) new TestGraphDatabaseFactory()
                    .newImpermanentDatabaseBuilder()
                    .setConfig(GraphDatabaseSettings.load_csv_file_url_root, import_folder.toString())
                    .setConfig("apoc.import.file.enabled", String.valueOf(testName.getMethodName().contains("WithImportEnabled")))
                    .setConfig("apoc.import.file.use_neo4j_config", String.valueOf(testName.getMethodName().contains("WithImportUsingNeo4jConf")))
                    .setConfig("dbms.security.allow_csv_import_from_file_urls", String.valueOf(testName.getMethodName().contains("WithAllowReadFs")))
                    .newGraphDatabase();
            TestUtil.registerProcedure(db, LoadJson.class, Xml.class, LoadXls.class, LoadHtml.class, LoadCsv.class);
        }

        @After
        public void shutdown() {
            if (db != null) {
                db.shutdown();
            }
        }

        @Parameterized.Parameters
        public static Collection<String[]> data() {
            return LoadSecurityTest.data();
        }

        @Test
        public void testIllegalFSAccess() {
            final String message = apocProcedure + " should throw an exception";
            try {
                db.execute("CALL " + apocProcedure, Util.map("fileName", "./hello")).resultAsString();
                fail(message);
            } catch (Exception e) {
                assertError(e, FileUtils.LOAD_FROM_FILE_ERROR, RuntimeException.class, apocProcedure);
            }
        }

        @Test
        public void testIllegalFSAccessWithImportEnabledAndWithImportUsingNeo4jConf() {
            final String message = apocProcedure + " should throw an exception";
            final String fileName = SensitivePathGenerator.etcPasswd(db).first();
            try {
                db.execute("CALL " + apocProcedure, Util.map("fileName", fileName)).resultAsString();
                fail(message);
            } catch (Exception e) {
                assertError(e, String.format(FileUtils.ERROR_READ_FROM_FS_NOT_ALLOWED, fileName), RuntimeException.class, apocProcedure);
            }
        }

        @Test
        public void testReadSensitiveFileWorksWithImportEnabledAndWithAllowReadFs() {
            final String fileName = SensitivePathGenerator.etcPasswd(db).first();
            try {
                db.execute("CALL " + apocProcedure, Util.map("fileName", fileName)).resultAsString();
                if (ALLOWED_EXCEPTIONS.containsKey(exportMethod)) {
                    fail("Expected to fail by throwing the following exception: " + ALLOWED_EXCEPTIONS.get(exportMethod));
                }
            } catch (Exception e) {
                if (ALLOWED_EXCEPTIONS.containsKey(exportMethod)) {
                    assertEquals(apocProcedure + " should", ALLOWED_EXCEPTIONS.get(exportMethod), ExceptionUtils.getRootCause(e).getClass());
                }
            }
        }
    }

    private static void assertError(Exception e, String errorMessage, Class<? extends Exception> exceptionType, String apocProcedure) {
        final Throwable rootCause = ExceptionUtils.getRootCause(e);
        assertEquals(apocProcedure + " should throw the following message", errorMessage, rootCause.getMessage());
        assertTrue(apocProcedure + " should throw an instance of " + exceptionType.getSimpleName(), exceptionType.isInstance(rootCause));
    }

}