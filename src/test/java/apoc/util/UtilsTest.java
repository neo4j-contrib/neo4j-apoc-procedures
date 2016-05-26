package apoc.util;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.junit.Assert.assertEquals;

/**
 * @author mh
 * @since 26.05.16
 */
public class UtilsTest {

    private static GraphDatabaseService db;

    @BeforeClass
    public static void setUp() throws Exception {
        db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        TestUtil.registerProcedure(db, Utils.class);
    }

    @AfterClass
    public static void tearDown() {
        db.shutdown();
    }

    @Test
    public void testSha1() throws Exception {
        TestUtil.testCall(db, "call apoc.util.sha1(['ABC'])", r -> assertEquals("3c01bdbb26f358bab27f267924aa2c9a03fcfdb8",r.get("value")));
    }

    @Test
    public void testMd5() throws Exception {
        TestUtil.testCall(db, "call apoc.util.md5(['ABC'])", r -> assertEquals("902fbdd2b1df0c4f70b4a5d23525e932",r.get("value")));
    }

}
