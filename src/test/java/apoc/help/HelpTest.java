package apoc.help;

import apoc.bitwise.BitwiseOperations;
import apoc.meta.Meta;
import apoc.util.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.test.TestGraphDatabaseFactory;

import static apoc.util.Util.map;
import static org.junit.Assert.*;

/**
 * @author mh
 * @since 06.11.16
 */
public class HelpTest {

    private GraphDatabaseService db;

    @Before
    public void setUp() throws Exception {
        db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        TestUtil.registerProcedure(db, Help.class, BitwiseOperations.class, Meta.class);
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
        TestUtil.testCall(db,"CALL apoc.help({text})",map("text","types"), (row) -> {
            assertEquals("function",row.get("type"));
            assertEquals("apoc.meta.types",row.get("name"));
            assertEquals(true, ((String) row.get("text")).contains("map of keys to types"));
        });
    }

}
