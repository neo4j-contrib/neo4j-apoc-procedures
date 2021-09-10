package apoc.export;

import apoc.ApocConfig;
import apoc.ApocSettings;
import apoc.export.csv.ExportCSV;
import apoc.export.cypher.ExportCypher;
import apoc.export.graphml.ExportGraphML;
import apoc.export.json.ExportJson;
import apoc.util.TestUtil;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.Result;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @author mh
 * @since 22.05.16
 */
@RunWith(Parameterized.class)
public class ExportCoreSecurityTest {

    private static File directory = new File("target/import");

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

    private final String exportMethod;

    public ExportCoreSecurityTest(String exportMethod) {
        this.exportMethod = exportMethod;
    }


    @Parameterized.Parameters
    public static Collection<String> data() {
        return Arrays.asList("csv", "json", "graphml", "cypher");
    }

    @Test
    public void testIllegalFSAccessExportQuery() {
        final String message = "apoc.export." + exportMethod + ".query should throw an exception";
        try {
            db.executeTransactionally("CALL apoc.export." + exportMethod + ".query(\"RETURN 'hello' as key\", './hello', {})", Map.of(),
                    Result::resultAsString);
            fail(message);
        } catch (Exception e) {
            assertExportError(e);
        }
        try {
            db.executeTransactionally("CALL apoc.export." + exportMethod + ".query(\"RETURN 'hello' as key\", './hello', {stream:true})", Map.of(),
                    Result::resultAsString);
            fail(message);
        } catch (Exception e) {
            assertExportError(e);
        }
        try {
            db.executeTransactionally("CALL apoc.export." + exportMethod + ".query(\"RETURN 'hello' as key\", '  ', {})", Map.of(),
                    Result::resultAsString);
            fail(message);
        } catch (Exception e) {
            assertExportError(e);
        }
    }

    @Test
    public void testIllegalFSAccessExportAll() {
        final String message = "apoc.export." + exportMethod + ".all should throw an exception";
        try {
            db.executeTransactionally("CALL apoc.export." + exportMethod + ".all('./hello', {})", Map.of(),
                    Result::resultAsString);
            fail(message);
        } catch (Exception e) {
            assertExportError(e);
        }
        try {
            db.executeTransactionally("CALL apoc.export." + exportMethod + ".all('./hello', {stream:true})", Map.of(),
                    Result::resultAsString);
            fail(message);
        } catch (Exception e) {
            assertExportError(e);
        }
        try {
            db.executeTransactionally("CALL apoc.export." + exportMethod + ".all('  ', {})", Map.of(),
                    Result::resultAsString);
            fail(message);
        } catch (Exception e) {
            assertExportError(e);
        }
    }

    @Test
    public void testIllegalFSAccessExportData() {
        final String message = "apoc.export." + exportMethod + ".data should throw an exception";
        try {
            db.executeTransactionally("CALL apoc.export." + exportMethod + ".data([], [], './hello', {})", Map.of(),
                    Result::resultAsString);
            fail(message);
        } catch (Exception e) {
            assertExportError(e);
        }
        try {
            db.executeTransactionally("CALL apoc.export." + exportMethod + ".data([], [], './hello', {stream:true})", Map.of(),
                    Result::resultAsString);
            fail(message);
        } catch (Exception e) {
            assertExportError(e);
        }
        try {
            db.executeTransactionally("CALL apoc.export." + exportMethod + ".data([], [], '  ', {})", Map.of(),
                    Result::resultAsString);
            fail(message);
        } catch (Exception e) {
            assertExportError(e);
        }
    }

    @Test
    public void testIllegalFSAccessExportGraph() {
        final String message = "apoc.export." + exportMethod + ".data should throw an exception";
        try {
            db.executeTransactionally("CALL apoc.export." + exportMethod + ".graph({nodes: [], relationships: []}, './hello', {})", Map.of(),
                    Result::resultAsString);
            fail(message);
        } catch (Exception e) {
            assertExportError(e);
        }
        try {
            db.executeTransactionally("CALL apoc.export." + exportMethod + ".graph({nodes: [], relationships: []}, './hello', {stream:true})", Map.of(),
                    Result::resultAsString);
            fail(message);
        } catch (Exception e) {
            assertExportError(e);
        }
        try {
            db.executeTransactionally("CALL apoc.export." + exportMethod + ".graph({nodes: [], relationships: []}, '  ', {})", Map.of(),
                    Result::resultAsString);
            fail(message);
        } catch (Exception e) {
            assertExportError(e);
        }
    }

    @Test
    public void testIllegalFSAccessExportCypherSchema() {
        Assume.assumeTrue(exportMethod.equals("cypher"));
        try {
            final String message = "apoc.export." + exportMethod + ".schema should throw an exception";
            db.executeTransactionally("CALL apoc.export." + exportMethod + ".schema('./hello', {})", Map.of(),
                    Result::resultAsString);
            fail(message);
        } catch (Exception e) {
            assertExportError(e);
        }
    }



    private void assertExportError(Exception e) {
        final Throwable rootCause = ExceptionUtils.getRootCause(e);
        assertTrue("it should be an instance of Runtime Exception", rootCause instanceof RuntimeException);
        assertEquals(ApocConfig.EXPORT_TO_FILE_ERROR, rootCause.getMessage());
    }

}
