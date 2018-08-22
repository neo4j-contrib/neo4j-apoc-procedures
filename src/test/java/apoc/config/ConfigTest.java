package apoc.config;

import apoc.util.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.junit.Assert.assertEquals;

/**
 * @author mh
 * @since 28.10.16
 */
public class ConfigTest {

    private GraphDatabaseService db;

    @Before
    public void setUp() throws Exception {
        db = new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder()
                .setConfig("foo", "bar")
                .newGraphDatabase();
        TestUtil.registerProcedure(db, Config.class);
    }

    @After
    public void tearDown(){
        db.shutdown();
    }

    @Test
    public void listTest(){
        TestUtil.testCall(db, "CALL apoc.config.list() yield key with * where key STARTS WITH 'foo' RETURN *",(row) -> assertEquals("foo",row.get("key")));
    }

}