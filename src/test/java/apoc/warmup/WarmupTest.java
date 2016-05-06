package apoc.warmup;

import apoc.util.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.junit.Assert.assertEquals;

/**
 * @author Sascha Peukert
 * @since 06.05.16
 */
public class WarmupTest {

    private GraphDatabaseService db;

    @Before
    public void setUp() throws Exception {
        db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        TestUtil.registerProcedure(db,Warmup.class);
    }
    @After
    public void tearDown() {
        db.shutdown();
    }

    @Test
    public void testWarmup() throws Exception {
        db.execute("CREATE (n)-[:KNOWS]->(m)").close();
        TestUtil.testCall(db,"CALL apoc.warmup.run()", r ->
        {
            assertEquals(2L, r.get("nodesTotal"));
            assertEquals(1L, r.get("nodesLoaded"));
            assertEquals(1L, r.get("relsTotal"));
            assertEquals(1L, r.get("relsLoaded"));
        });
    }
}
