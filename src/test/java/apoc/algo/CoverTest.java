package apoc.algo;

import apoc.es.ElasticSearch;
import apoc.util.TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.junit.Assert.*;

/**
 * @author mh
 * @since 21.05.16
 */
public class CoverTest {

    private static GraphDatabaseService db;

    @BeforeClass
    public static void setUp() throws Exception {
        db = new TestGraphDatabaseFactory()
                .newImpermanentDatabaseBuilder()
                .newGraphDatabase();
        TestUtil.registerProcedure(db, Cover.class);
        db.execute("CREATE (a)-[:X]->(b)-[:X]->(c)-[:X]->(d)").close();
    }

    @AfterClass
    public static void tearDown() {
        db.shutdown();
    }

    @Test
    public void testCover() throws Exception {
        TestUtil.testCall(db,
                "match (n) with collect(id(n)) as nodes call apoc.algo.cover(nodes) yield rel return count(*) as c",
                (r) -> assertEquals(3L,r.get("c")));
    }
}
