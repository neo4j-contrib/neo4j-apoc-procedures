package apoc;

import apoc.export.csv.ExportCSV;
import apoc.export.csv.ImportCsv;
import apoc.export.graphml.ExportGraphML;
import apoc.export.json.ExportJson;
import apoc.export.json.ImportJson;
import apoc.load.LoadCsv;
import apoc.load.LoadJson;
import apoc.periodic.Periodic;
import apoc.util.TestUtil;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.stream.IntStream;

import static apoc.kernel.KernelTestUtils.checkStatusDetails;
import static org.neo4j.configuration.GraphDatabaseSettings.TransactionStateMemoryAllocation.OFF_HEAP;
import static org.neo4j.configuration.SettingValueParsers.BYTES;

@RunWith(Parameterized.class)
public class StatusDetailsTest {

    private static File directory = new File("target/import");

    static { //noinspection ResultOfMethodCallIgnored
        directory.mkdirs();
    }

    @ClassRule
    public static DbmsRule db;

    static {
        try {
            db = new ImpermanentDbmsRule()
                    .withSetting(GraphDatabaseSettings.memory_tracking, true)
                    .withSetting(GraphDatabaseSettings.tx_state_memory_allocation, OFF_HEAP)
                    .withSetting(GraphDatabaseSettings.tx_state_max_off_heap_memory, BYTES.parse("2G"))
                    .withSetting(ApocSettings.apoc_import_file_enabled, true)
                    .withSetting(ApocSettings.apoc_export_file_enabled, true)
                    .withSetting(GraphDatabaseSettings.load_csv_file_url_root, directory.toPath().toAbsolutePath());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @BeforeClass
    public static void setUp() throws Exception {
        db.executeTransactionally("CREATE CONSTRAINT ON (n:Status) assert n.neo4jImportId IS UNIQUE");
        TestUtil.registerProcedure(db, Periodic.class,
                ExportJson.class, ImportJson.class, LoadJson.class, 
                ExportCSV.class, ImportCsv.class, LoadCsv.class, 
                ExportGraphML.class);
    }

    @AfterClass
    public static void tearDown() {
        db.shutdown();
    }
    
    @Before
    public void before() throws Exception {
        IntStream.range(0, 10)
                .forEach(__-> db.executeTransactionally("UNWIND range(0, 10000) AS x CREATE (:Status)"));
    }
    
    @After
    public void after() throws Exception {
        db.executeTransactionally("CALL apoc.periodic.truncate");
    }

    @Parameterized.Parameters
    public static Collection<String[]> data() {
        return Arrays.asList(new String[][] {
                { "status.csv",
                        "CALL apoc.export.csv.all($file, {})",
                        "CALL apoc.import.csv([{fileName: $file, labels: ['Status']}], [], {batchSize: 500})",
                        "CALL apoc.load.csv($file,null)" },
                { "status.json",
                        "CALL apoc.export.json.all($file, {})",
                        "CALL apoc.import.json($file, {batchSize: 500})",
                        "CALL apoc.load.json($file)" },
                { "status.graphml" ,
                        "MATCH (n:Status) WITH collect(n) as nodes CALL apoc.export.graphml.data(nodes, [], $file, {}) yield data RETURN 1",
                        "CALL apoc.import.graphml($file,{readLabels:true, batchSize: 500})",
                        null }
        });
    }

    @Parameterized.Parameter(0)
    public String file;

    @Parameterized.Parameter(1)
    public String exportQuery;

    @Parameterized.Parameter(2)
    public String importQuery;

    @Parameterized.Parameter(3)
    public String loadQuery;

    
    @Test
    public void testImportExportStatusDetails() {
        checkStatus(exportQuery);
        checkStatus(importQuery);
        checkStatus(loadQuery);
    }

    private void checkStatus(String query) {
        if (query != null) {
            checkStatusDetails(db, query, Map.of("file", file));
        }
    }
}
