package apoc.help;

import apoc.bitwise.BitwiseOperations;
import apoc.coll.Coll;
import apoc.util.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.test.TestGraphDatabaseFactory;

import static apoc.util.Util.map;
import static org.junit.Assert.assertEquals;

/**
 * @author mh
 * @since 06.11.16
 */
public class HelpTest {

    private GraphDatabaseService db;

    @Before
    public void setUp() throws Exception {
        db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        TestUtil.registerProcedure(db, Help.class, BitwiseOperations.class,Coll.class);
    }

    @After
    public void tearDown() {
        db.shutdown();
    }

    @Test
    public void info() throws Exception {
        TestUtil.testCall(db,"CALL apoc.help({text})",map("text","bitwise"), (row) -> {
            assertEquals("function",row.get("type"));
            assertEquals("apoc.bitwise.op",row.get("name"));
            assertEquals(true, ((String) row.get("text")).contains("bitwise operations"));
        });
        TestUtil.testCall(db,"CALL apoc.help({text})",map("text","operations+"), (row) -> assertEquals("apoc.bitwise.op",row.get("name")));
        TestUtil.testCall(db,"CALL apoc.help({text})",map("text","toSet"), (row) -> {
            assertEquals("function",row.get("type"));
            assertEquals("apoc.coll.toSet",row.get("name"));
            assertEquals(true, ((String) row.get("text")).contains("unique list"));
        });
    }

}
