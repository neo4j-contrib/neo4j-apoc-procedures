package apoc.warmup;

import apoc.util.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.test.TestGraphDatabaseFactory;

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
        TestUtil.testCallEmpty(db,"CALL apoc.warmup.run()",null);
    }
}
