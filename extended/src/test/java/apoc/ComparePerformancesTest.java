package apoc;

import apoc.export.parquet.ImportParquet;
import apoc.export.csv.ExportCSV;
import apoc.export.csv.ImportCsv;
import apoc.export.parquet.ExportParquet;
import apoc.load.LoadParquet;
import apoc.meta.Meta;
import apoc.util.TestUtil;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.io.File;
import java.util.Map;
import java.util.stream.IntStream;

import static apoc.ApocConfig.APOC_EXPORT_FILE_ENABLED;
import static apoc.ApocConfig.APOC_IMPORT_FILE_ENABLED;
import static apoc.ApocConfig.apocConfig;
import static org.junit.Assert.assertEquals;
import static org.neo4j.configuration.GraphDatabaseSettings.TransactionStateMemoryAllocation.OFF_HEAP;
import static org.neo4j.configuration.SettingValueParsers.BYTES;

@Ignore("This test compare import/export procedures performances, we ignore it since it's slow and just log the times spent")
public class ComparePerformancesTest {
    private static final File directory = new File("target/import");
    static { //noinspection ResultOfMethodCallIgnored
        directory.mkdirs();
    }

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule()
            .withSetting(GraphDatabaseSettings.memory_tracking, true)
            .withSetting(GraphDatabaseSettings.tx_state_memory_allocation, OFF_HEAP)
            .withSetting(GraphDatabaseSettings.tx_state_max_off_heap_memory, BYTES.parse("4G"))
            .withSetting(GraphDatabaseSettings.load_csv_file_url_root, directory.toPath().toAbsolutePath());

    @Before
    public void setUp() throws Exception {
        TestUtil.registerProcedure(db, ImportParquet.class, ExportParquet.class, ExportCSV.class, Meta.class, ImportCsv.class, LoadParquet.class);
        apocConfig().setProperty(APOC_IMPORT_FILE_ENABLED, true);
        apocConfig().setProperty(APOC_EXPORT_FILE_ENABLED, true);

        IntStream.range(0, 50)
                .forEach(__-> db.executeTransactionally("UNWIND range(0, 19999) as id WITH id " +
                                                        "CREATE (:Start {idStart: id})-[:REL {idRel: id}]->(:End {idEnd: id})"));
    }

    @Test
    public void testPerformanceImportAndExportCsv() {
        exportCsv();
        importCsv();
    }

    @Test
    public void testPerformanceImportAndExportParquet() {
        exportParquet();
        importParquet();
    }

    private void exportParquet() {
        testPerformanceCommon("CALL apoc.export.parquet.all('test.parquet')", "endExportParquet = ");
    }

    private void exportCsv() {
        testPerformanceCommon("CALL apoc.export.csv.all('test.csv', {bulkImport: true})", "endExportCsv = ");
    }

    private void importCsv() {
        testPerformanceCommon("CALL apoc.import.csv([{fileName: 'test.nodes.Start.csv', labels: ['Start']}," +
                              "{fileName: 'test.nodes.End.csv', labels: ['End']}], [{fileName: 'test.relationships.REL.csv', type: 'REL'}], {}) ", "endImportCsv = ");
    }

    private void importParquet() {
        testPerformanceCommon("CALL apoc.import.parquet('test.parquet')", "endImportParquet = ");
    }

    private void testPerformanceCommon(String call, String printTime) {
        long start = System.currentTimeMillis();
        TestUtil.testCall(db, call, this::progressInfoAssertion);
        long end = System.currentTimeMillis() - start;
        System.out.println(printTime + end);
    }

    private void progressInfoAssertion(Map<String, Object> r) {
        assertEquals(2000000L, r.get("nodes"));
        assertEquals(1000000L, r.get("relationships"));
    }

}
