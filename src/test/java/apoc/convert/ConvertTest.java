package apoc.convert;

import apoc.coll.Coll;
import apoc.util.TestUtil;
import org.junit.*;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.Collections;

import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testCall;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.junit.Assert.*;

/**
 * @author mh
 * @since 29.05.16
 */
public class ConvertTest {


    private static GraphDatabaseService db;
    @BeforeClass
    public static void setUp() throws Exception {
        db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        TestUtil.registerProcedure(db,Convert.class);
    }
    @AfterClass
    public static void tearDown() {
        db.shutdown();
    }

    @Test
    public void testToMap() throws Exception {
        testCall(db, "call apoc.convert.toMap({a})",map("a",map("a","b")), r -> assertEquals(map("a","b"),r.get("value")));
        testCall(db, "call apoc.convert.toMap({a})",map("a",null), r -> assertEquals(null,r.get("value")));
    }

    @Test
    public void testToList() throws Exception {
        testCall(db, "call apoc.convert.toList({a})",map("a",null), r -> assertEquals(null,r.get("value")));
        testCall(db, "call apoc.convert.toList({a})",map("a",new Object[]{"a"}), r -> assertEquals(singletonList("a"),r.get("value")));
        testCall(db, "call apoc.convert.toList({a})",map("a",singleton("a")), r -> assertEquals(singletonList("a"),r.get("value")));
        testCall(db, "call apoc.convert.toList({a})",map("a",singletonList("a")), r -> assertEquals(singletonList("a"),r.get("value")));
        testCall(db, "call apoc.convert.toList({a})",map("a",singletonList("a").iterator()), r -> assertEquals(singletonList("a"),r.get("value")));
    }

    @Test
    public void testToBoolean() throws Exception {
        testCall(db, "call apoc.convert.toBoolean('true')", r -> assertEquals(true,r.get("value")));
        testCall(db, "call apoc.convert.toBoolean(1)", r -> assertEquals(true,r.get("value")));
        testCall(db, "call apoc.convert.toBoolean('yes')", r -> assertEquals(true,r.get("value")));
        testCall(db, "call apoc.convert.toBoolean('false')", r -> assertEquals(false,r.get("value")));
        testCall(db, "call apoc.convert.toBoolean(0)", r -> assertEquals(false,r.get("value")));
        testCall(db, "call apoc.convert.toBoolean('no')", r -> assertEquals(false,r.get("value")));
        testCall(db, "call apoc.convert.toBoolean('')", r -> assertEquals(false,r.get("value")));
        testCall(db, "call apoc.convert.toBoolean(null)", r -> assertEquals(false,r.get("value")));
    }

    @Test
    public void testToString() throws Exception {
        testCall(db, "call apoc.convert.toString(null)", r -> assertEquals(null,r.get("value")));
        testCall(db, "call apoc.convert.toString('a')", r -> assertEquals("a",r.get("value")));
        testCall(db, "call apoc.convert.toString(1)", r -> assertEquals("1",r.get("value")));
        testCall(db, "call apoc.convert.toString(true)", r -> assertEquals("true",r.get("value")));
        testCall(db, "call apoc.convert.toString([1])", r -> assertEquals("[1]",r.get("value")));
    }
    @Test
    public void testToNode() throws Exception {
        testCall(db, "CREATE (n) WITH [n] as x CALL apoc.convert.toNode(x[0]) YIELD node RETURN *",
                r -> assertEquals(true,r.get("node") instanceof Node));
        testCall(db, "CALL apoc.convert.toNode(null)", r -> assertEquals(null,r.get("node")));
    }

    @Test
    public void testToRelationship() throws Exception {
        testCall(db, "CREATE (n)-[r:KNOWS]->(m) WITH [r] as x CALL apoc.convert.toRelationship(x[0]) YIELD rel RETURN *",
                r -> assertEquals(true,r.get("rel") instanceof Relationship));
        testCall(db, "CALL apoc.convert.toRelationship(null)", r -> assertEquals(null,r.get("rel")));
    }

    @Test
    public void testToSet() throws Exception {
        testCall(db, "call apoc.convert.toSet({a})",map("a",null), r -> assertEquals(null,r.get("value")));
        testCall(db, "call apoc.convert.toSet({a})",map("a",new Object[]{"a"}), r -> assertEquals(singleton("a"),r.get("value")));
        testCall(db, "call apoc.convert.toSet({a})",map("a",singleton("a")), r -> assertEquals(singleton("a"),r.get("value")));
        testCall(db, "call apoc.convert.toSet({a})",map("a",singletonList("a")), r -> assertEquals(singleton("a"),r.get("value")));
        testCall(db, "call apoc.convert.toSet({a})",map("a",singletonList("a").iterator()), r -> assertEquals(singleton("a"),r.get("value")));
    }
}
