package apoc.load;

import apoc.util.GoogleCloudStorageContainerExtension;
import apoc.util.TestUtil;
import org.junit.*;

import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import static apoc.load.LoadCsvTest.assertRow;
import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testResult;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class LoadGoogleCloudStorageTest {

    private static final String GCS_URL = "http://localhost:4443/storage/v1";

    @ClassRule
    public static GoogleCloudStorageContainerExtension gcs = new GoogleCloudStorageContainerExtension()
            .withMountedResourceFile("test.csv", "/folder/test.csv")
            .withMountedResourceFile("map.json", "/folder/map.json");

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    @BeforeClass
    public static void setUp() throws Exception {
        TestUtil.registerProcedure(db, LoadCsv.class, LoadJson.class);
    }

    @AfterClass
    public static void tearDown() {
        gcs.close();
        db.shutdown();
    }

    @Test
    public void testLoadCsv() {
        String url = GCS_URL + "/b/folder/o/test.csv?alt=media";
        testResult(db, "CALL apoc.load.csv($url)", map("url", url), (r) -> {
            assertRow(r, "Selma", "8", 0L);
            assertRow(r, "Rana", "11", 1L);
            assertRow(r, "Selina", "18", 2L);
            assertFalse("It should be the last record", r.hasNext());
        });
    }

    @Test
    public void testLoadJSON() {
        String url = GCS_URL  + "/b/folder/o/map.json?alt=media";
        testCall(db, "CALL apoc.load.jsonArray($url, '$.foo')", map("url", url), (r) -> {
            assertEquals(asList(1L,2L,3L), r.get("value"));
        });
    }
}
