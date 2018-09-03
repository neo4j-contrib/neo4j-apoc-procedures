package apoc.uuid;

import apoc.util.TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.*;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

/**
 * @author ab-larus
 * @since 05.09.18
 */
public class UUIDTest {

    private static GraphDatabaseService db;

    @BeforeClass
    public static void setUp() throws Exception {
        db = TestUtil.apocGraphDatabaseBuilder()
                .setConfig("apoc.uuid.schedule", "5")
                .setConfig("apoc.uuid.enabled", "true")
                .setConfig("apoc.uuid.labels", "Bar")
                .newGraphDatabase();
        db.execute("CREATE (n:Bar)").close();
    }

    @AfterClass
    public static void tearDown() {
        db.shutdown();
    }

    @Test
    public void testUUID() throws Exception {
        Thread.sleep(10*1000);
        try (ResourceIterator<String> result = db.execute("CALL db.constraints").columnAs("description")) {
            assertTrue(result.hasNext());
            assertEquals("CONSTRAINT ON ( bar:Bar ) ASSERT bar.uuid IS UNIQUE", result.next());
            assertTrue(!result.hasNext());
        }
        try (Transaction tx = db.beginTx()) {
            Node n = (Node) db.execute("MATCH (n:Bar) return n").next().get("n");
            assertTrue(n.getAllProperties().containsKey("uuid"));
            assertTrue(n.getAllProperties().get("uuid").toString().matches("^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"));
            tx.success();
        }
    }

}