package apoc.cypher;

import apoc.util.TestUtil;
import apoc.util.Utils;
import org.junit.*;
import org.junit.rules.ExpectedException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.TransientTransactionFailureException;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testResult;
import static apoc.util.Util.toLong;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * @author mh
 * @since 08.05.16
 */
public class CypherTest {

    private static GraphDatabaseService db;

    @Rule
    public ExpectedException thrown= ExpectedException.none();

    @BeforeClass
    public static void setUp() throws Exception {
        db = new TestGraphDatabaseFactory()
                .newImpermanentDatabaseBuilder()
                .setConfig("apoc.import.file.enabled", "true")
                .setConfig(GraphDatabaseSettings.load_csv_file_url_root,new File("src/test/resources").getAbsolutePath())
                .newGraphDatabase();
        TestUtil.registerProcedure(db, Cypher.class);
        TestUtil.registerProcedure(db, Utils.class);
    }

    @AfterClass
    public static void tearDown() {
        db.shutdown();
    }

    @After
    public void clearDB() {
        db.execute("MATCH (n) DETACH DELETE n");
    }


    @Test
    public void testRun() throws Exception {
        testCall(db, "CALL apoc.cypher.run('RETURN {a} + 7 as b',{a:3})",
                r -> assertEquals(10L, ((Map) r.get("value")).get("b")));
    }
    @Test
    public void testRunNullParams() throws Exception {
        testCall(db, "CALL apoc.cypher.run('RETURN 42 as b',null)",
                r -> assertEquals(42L, ((Map) r.get("value")).get("b")));
    }
    @Test
    public void testRunNoParams() throws Exception {
        testCall(db, "CALL apoc.cypher.run('RETURN 42 as b',{})",
                r -> assertEquals(42L, ((Map) r.get("value")).get("b")));
    }

    @Test
    public void testRunVariable() throws Exception {
        testCall(db, "CALL apoc.cypher.run('RETURN a + 7 as b',{a:3})",
                r -> assertEquals(10L, ((Map) r.get("value")).get("b")));
    }

    @Test
    public void testParallel() throws Exception {
        int size = 10_000;
        testResult(db, "CALL apoc.cypher.parallel2('UNWIND range(0,9) as b RETURN b',{a:range(1,{size})},'a')", map("size", size),
                r -> assertEquals( size * 10,Iterators.count(r) ));
    }
    @Test
    public void testSingular() throws Exception {
        int size = 10_000;
        testResult(db, "CALL apoc.cypher.run('UNWIND a as row UNWIND range(0,9) as b RETURN b',{a:range(1,{size})})", map("size", size),
                r -> assertEquals( size * 10,Iterators.count(r) ));
    }
    @Test
    public void testMapParallel() throws Exception {
        int size = 10_000;
        testResult(db, "CALL apoc.cypher.mapParallel('UNWIND range(0,9) as b RETURN b',{},range(1,{size}))", map("size", size),
                r -> assertEquals( size * 10,Iterators.count(r) ));
    }
    @Test
    public void testMapParallel2() throws Exception {
        int size = 10_000;
        testResult(db, "CALL apoc.cypher.mapParallel2('UNWIND range(0,9) as b RETURN b',{},range(1,{size}),10)", map("size", size),
                r -> assertEquals( size * 10,Iterators.count(r) ));
    }
    @Test
    public void testParallel2() throws Exception {
        int size = 10_0000;
        List<Long> list = new ArrayList<>(size);
        for (int i = 0; i < size; i++) list.add(3L);
        testCall(db, "CALL apoc.cypher.parallel2('RETURN a + 7 as b',{a:{list}},'a') YIELD value RETURN sum(value.b) as b", map("list", list),
                r -> {
                    assertEquals( size * 10L, r.get("b") );
                });
    }


    @Test
    public void testRunMany() throws Exception {
        testResult(db, "CALL apoc.cypher.runMany('CREATE (n:Node {name:{name}});\nMATCH (n {name:{name}}) CREATE (n)-[:X {name:{name2}}]->(n);',{params})",map("params",map("name","John","name2","Doe")),
                r -> {
                    Map<String, Object> row = r.next();
                    assertEquals(-1L, row.get("row"));
                    Map result = (Map) row.get("result");
                    assertEquals(1L, toLong(result.get("nodesCreated")));
                    assertEquals(1L, toLong(result.get("labelsAdded")));
                    assertEquals(1L, toLong(result.get("propertiesSet")));
                    row = r.next();
                    result = (Map) row.get("result");
                    assertEquals(-1L, row.get("row"));
                    assertEquals(1L, toLong(result.get("relationshipsCreated")));
                    assertEquals(1L, toLong(result.get("propertiesSet")));
                    assertEquals(false, r.hasNext());
                });
    }
    @Test
    public void testRunFile() throws Exception {
        testResult(db, "CALL apoc.cypher.runFile('src/test/resources/create_delete.cypher')",
                r -> {
                    Map<String, Object> row = r.next();
                    assertEquals(-1L, row.get("row"));
                    Map result = (Map) row.get("result");
                    assertEquals(1L, toLong(result.get("nodesCreated")));
                    assertEquals(1L, toLong(result.get("labelsAdded")));
                    assertEquals(1L, toLong(result.get("propertiesSet")));
                    row = r.next();
                    result = (Map) row.get("result");
                    assertEquals(-1L, row.get("row"));
                    assertEquals(1L, toLong(result.get("nodesDeleted")));
                    assertEquals(false, r.hasNext());
                });
    }
    @Test
    public void testRunWithPeriodic() throws Exception {
        testResult(db, "CALL apoc.cypher.runFile('src/test/resources/periodic.cypher')",
                r -> {
                    Map<String, Object> row = r.next();
                    assertEquals(-1L, row.get("row"));
                    Map result = (Map) row.get("result");
                    assertEquals(1L, toLong(result.get("nodesCreated")));
                    assertEquals(1L, toLong(result.get("labelsAdded")));
                    assertEquals(2L, toLong(result.get("propertiesSet")));
                    assertEquals(false, r.hasNext());
                });
    }

    @Test(expected = QueryExecutionException.class)
    public void testRunFileWithSchema() throws Exception {
        testResult(db, "CALL apoc.cypher.runFile('src/test/resources/schema_create.cypher')",
                r -> {
                    Map<String, Object> row = r.next();
                    assertEquals(-1L, row.get("row"));
                    Map result = (Map) row.get("result");
                    assertEquals(1L, toLong(result.get("indexesAdded")));


                    row = r.next();
                    assertEquals(-1L, row.get("row"));
                    result = (Map) row.get("result");
                    assertEquals(1L, toLong(result.get("nodesCreated")));
                    assertEquals(1L, toLong(result.get("labelsAdded")));
                    assertEquals(1L, toLong(result.get("propertiesSet")));
                    assertEquals(false, r.hasNext());
                });
    }
    @Test
    public void testRunFileWithResults() throws Exception {
        testResult(db, "CALL apoc.cypher.runFile('src/test/resources/create.cypher')",
                r -> {
                    Map<String, Object> row = r.next();
                    assertEquals(row.get("row"),((Map)row.get("result")).get("id"));
                    row = r.next();
                    assertEquals(row.get("row"),((Map)row.get("result")).get("id"));
                    row = r.next();
                    assertEquals(row.get("row"),((Map)row.get("result")).get("id"));
                    row = r.next();
                    assertEquals(-1L, row.get("row"));
                    Map result = (Map) row.get("result");
                    assertEquals(3L, toLong(result.get("nodesCreated")));
                    assertEquals(3L, toLong(result.get("labelsAdded")));
                    assertEquals(3L, toLong(result.get("propertiesSet")));

                    row = r.next();
                    result = (Map) row.get("result");
                    assertEquals(-1L, row.get("row"));
                    assertEquals(3L, toLong(result.get("nodesDeleted")));
                    assertEquals(false, r.hasNext());
                });
    }

    @Test(timeout=9000)
    public void testWithTimeout() {
        thrown.expect(TransientTransactionFailureException.class);
        thrown.expectMessage("Explicitly terminated by the user.");
        Result result = db.execute("CALL apoc.cypher.runTimeboxed('CALL apoc.util.sleep(10000)', null, {timeout})", Collections.singletonMap("timeout", 100));
        assertFalse(result.hasNext());
    }

    @Test
    public void testSimpleWhenIfCondition() throws Exception {
        testCall(db, "CALL apoc.when(true, 'RETURN 7 as b')",
                r -> assertEquals(7L, ((Map) r.get("value")).get("b")));
    }

    @Test
    public void testSimpleWhenElseCondition() throws Exception {
        testCall(db, "CALL apoc.when(false, 'RETURN 7 as b') YIELD value RETURN value",
                r -> assertEquals(null, ((Map) r.get("value")).get("b")));
    }

    @Test
    public void testWhenIfCondition() throws Exception {
        testCall(db, "CALL apoc.when(true, 'RETURN {a} + 7 as b', 'RETURN {a} as b',{a:3})",
                r -> assertEquals(10L, ((Map) r.get("value")).get("b")));
    }

    @Test
    public void testWhenElseCondition() throws Exception {
        testCall(db, "CALL apoc.when(false, 'RETURN {a} + 7 as b', 'RETURN {a} as b',{a:3})",
                r -> assertEquals(3L, ((Map) r.get("value")).get("b")));
    }

    @Test
    public void testDoWhenIfCondition() throws Exception {
        testCall(db, "CALL apoc.when.do(true, 'CREATE (a:Node{name:\"A\"}) RETURN a.name as aName', 'CREATE (b:Node{name:\"B\"}) RETURN b.name as bName',{})",
                r -> {
                    assertEquals("A", ((Map) r.get("value")).get("aName"));
                    assertEquals(null, ((Map) r.get("value")).get("bName"));
                });
    }

    @Test
    public void testDoWhenElseCondition() throws Exception {
        testCall(db, "CALL apoc.when.do(false, 'CREATE (a:Node{name:\"A\"}) RETURN a.name as aName', 'CREATE (b:Node{name:\"B\"}) RETURN b.name as bName',{})",
                r -> {
                    assertEquals("B", ((Map) r.get("value")).get("bName"));
                    assertEquals(null, ((Map) r.get("value")).get("aName"));
                });
    }

    @Test
    public void testWhenMany() throws Exception {
        testCall(db, "CALL apoc.whenMany([[false, 'RETURN {a} + 7 as b'], [false, 'RETURN {a} as b'], [true, 'RETURN {a} + 4 as b'], [false, 'RETURN {a} + 1 as b']], 'RETURN {a} + 10 as b', {a:3})",
                r -> assertEquals(7L, ((Map) r.get("value")).get("b")));
    }

    @Test
    public void testWhenManyElseCondition() throws Exception {
        testCall(db, "CALL apoc.whenMany([[false, 'RETURN {a} + 7 as b'], [false, 'RETURN {a} as b'], [false, 'RETURN {a} + 4 as b']], 'RETURN {a} + 10 as b', {a:3})",
                r -> assertEquals(13L, ((Map) r.get("value")).get("b")));
    }

    @Test
    public void testSimpleWhenMany() throws Exception {
        testCall(db, "CALL apoc.whenMany([[false, 'RETURN 3 + 7 as b'], [false, 'RETURN 3 as b'], [true, 'RETURN 3 + 4 as b']])",
                r -> assertEquals(7L, ((Map) r.get("value")).get("b")));
    }

    @Test
    public void testSimpleWhenManyElseCondition() throws Exception {
        testCall(db, "CALL apoc.whenMany([[false, 'RETURN 3 + 7 as b'], [false, 'RETURN 3 as b'], [false, 'RETURN 3 + 4 as b']], 'RETURN 3 + 10 as b')",
                r -> assertEquals(13L, ((Map) r.get("value")).get("b")));
    }

    @Test
    public void testDoWhenMany() throws Exception {
        testCall(db, "CALL apoc.whenMany.do([[false, 'CREATE (a:Node{name:\"A\"}) RETURN a.name as aName'], [true, 'CREATE (b:Node{name:\"B\"}) RETURN b.name as bName']], 'CREATE (c:Node{name:\"C\"}) RETURN c.name as cName',{})",
                r -> {
                    assertEquals(null, ((Map) r.get("value")).get("aName"));
                    assertEquals("B", ((Map) r.get("value")).get("bName"));
                    assertEquals(null, ((Map) r.get("value")).get("cName"));
                });
    }

    @Test
    public void testDoWhenManyElseCondition() throws Exception {
        testCall(db, "CALL apoc.whenMany.do([[false, 'CREATE (a:Node{name:\"A\"}) RETURN a.name as aName'], [false, 'CREATE (b:Node{name:\"B\"}) RETURN b.name as bName']], 'CREATE (c:Node{name:\"C\"}) RETURN c.name as cName',{})",
                r -> {
                    assertEquals(null, ((Map) r.get("value")).get("aName"));
                    assertEquals(null, ((Map) r.get("value")).get("bName"));
                    assertEquals("C", ((Map) r.get("value")).get("cName"));
                });
    }
}
