package apoc.es;

import apoc.cypher.Cypher;
import apoc.util.TestUtil;
import org.junit.*;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.io.File;
import java.net.ConnectException;

import static org.junit.Assert.*;

/**
 * @author mh
 * @since 21.05.16
 */
public class ElasticSearchTest {

    private static GraphDatabaseService db;

    @BeforeClass
    public static void setUp() throws Exception {
        db = new TestGraphDatabaseFactory()
                .newImpermanentDatabaseBuilder()
                .newGraphDatabase();
        TestUtil.registerProcedure(db, ElasticSearch.class);
    }

    @AfterClass
    public static void tearDown() {
        db.shutdown();
    }

    @Test
    public void testStats() throws Exception {
        TestUtil.ignoreException(() -> {
            TestUtil.testCall(db, "CALL apoc.es.stats(null)", r -> assertNotNull(r.get("value")));
        }, ConnectException.class);
    }
}
