package apoc.map;

import apoc.util.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.ArrayList;
import java.util.Map;

import static apoc.util.MapUtil.map;
import static java.util.Arrays.asList;
import static org.junit.Assert.*;

/**
 * @author mh
 * @since 04.05.16
 */
public class MapsTest {

    private GraphDatabaseService db;
    @Before
    public void setUp() throws Exception {
        db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        TestUtil.registerProcedure(db,Maps.class);
    }
    @After
    public void tearDown() {
        db.shutdown();
    }

    @Test
    public void testFromNodes() throws Exception {
        db.execute("UNWIND range(1,3) as id create (:Person {name:'name'+id})").close();
        TestUtil.testCall(db, "RETURN apoc.map.fromNodes('Person','name') as value", (r) -> {
            Map<String,Node> map = (Map<String, Node>) r.get("value");
            assertEquals(asList("name1","name2","name3"),new ArrayList<>(map.keySet()));
            map.forEach((k,v) -> assertEquals(k, v.getProperty("name")));
        });
    }
    @Test
    public void testGroupBy() throws Exception {
        TestUtil.testCall(db, "RETURN apoc.map.groupBy([{id:0,a:1},{id:1, b:false},{id:0,c:2}],'id') as value", (r) -> {
            assertEquals(map("0",map("id",0L,"c",2L),"1",map("id",1L,"b",false)),r.get("value"));
        });
    }
    @Test
    public void testGroupByMulti() throws Exception {
        TestUtil.testCall(db, "RETURN apoc.map.groupByMulti([{id:0,a:1},{id:1, b:false},{id:0,c:2}],'id') as value", (r) -> {
            assertEquals(map("0",asList(map("id",0L,"a",1L),map("id",0L,"c",2L)),"1",asList(map("id",1L,"b",false))),r.get("value"));
        });
    }
    @Test
    public void testMerge() throws Exception {
        TestUtil.testCall(db, "RETURN apoc.map.merge({a:1},{b:false}) AS value", (r) -> {
            assertEquals(map("a",1L,"b",false),r.get("value"));
        });
    }

    @Test
    public void testMergeList() throws Exception {
        TestUtil.testCall(db, "RETURN apoc.map.mergeList([{a:1},{b:false}]) as value", (r) -> {
            assertEquals(map("a",1L,"b",false),r.get("value"));
        });
    }

    @Test
    public void testFromPairs() throws Exception {
        TestUtil.testCall(db, "RETURN apoc.map.fromPairs([['a',1],['b',false]]) AS value", (r) -> {
            assertEquals(map("a",1L,"b",false),r.get("value"));
        });
    }
    @Test
    public void testFromValues() throws Exception {
        TestUtil.testCall(db, "RETURN apoc.map.fromValues(['a',1,'b',false]) AS value", (r) -> {
            assertEquals(map("a",1L,"b",false),r.get("value"));
        });
    }

    @Test
    public void testFromLists() throws Exception {
        TestUtil.testCall(db, "RETURN apoc.map.fromLists(['a','b'],[1,false]) AS value", (r) -> {
            assertEquals(map("a",1L,"b",false),r.get("value"));
        });
    }
    @Test
    public void testSetPairs() throws Exception {
        TestUtil.testCall(db, "RETURN apoc.map.setPairs({}, [['a',1],['b',false]]) AS value", (r) -> {
            assertEquals(map("a",1L,"b",false),r.get("value"));
        });
    }
    @Test
    public void testSetValues() throws Exception {
        TestUtil.testCall(db, "RETURN apoc.map.setValues({}, ['a',1,'b',false]) AS value", (r) -> {
            assertEquals(map("a",1L,"b",false),r.get("value"));
        });
    }

    @Test
    public void testSetLists() throws Exception {
        TestUtil.testCall(db, "RETURN apoc.map.setLists({}, ['a','b'],[1,false]) AS value", (r) -> {
            assertEquals(map("a",1L,"b",false),r.get("value"));
        });
    }

    @Test
    public void testSetKey() throws Exception {
        TestUtil.testCall(db, "RETURN apoc.map.setKey({a:1},'a',2) AS value", (r) -> {
            assertEquals(map("a",2L),r.get("value"));
        });
    }
    @Test
    public void testSetEntry() throws Exception {
        TestUtil.testCall(db, "RETURN apoc.map.setEntry({a:1},'a',2) AS value", (r) -> {
            assertEquals(map("a",2L),r.get("value"));
        });
    }

    @Test
    public void testRemoveKey() throws Exception {
        TestUtil.testCall(db, "RETURN apoc.map.removeKey({a:1,b:2},'a') AS value", (r) -> {
            assertEquals(map("b",2L),r.get("value"));
        });
    }
    @Test
    public void testRemoveKeys() throws Exception {
        TestUtil.testCall(db, "RETURN apoc.map.removeKeys({a:1,b:2},['a','b']) AS value", (r) -> {
            assertEquals(map(),r.get("value"));
        });
    }

    @Test
    public void testClean() throws Exception {
        TestUtil.testCall(db, "RETURN apoc.map.clean({a:1,b:'',c:null,x:1234,z:false},['x'],['',false]) AS value", (r) -> {
            assertEquals(map("a",1L),r.get("value"));
        });
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testFlatten() {
        Map<String, Object> nestedMap = map("somekey", "someValue", "somenumeric", 123);
        nestedMap = map("anotherkey", "anotherValue", "nested", nestedMap);
        Map<String, Object> map = map("string", "value", "int", 10, "nested", nestedMap);

        TestUtil.testCall(db, "RETURN apoc.map.flatten({map}) AS value", map("map", map), (r) -> {
            Map<String, Object> resultMap = (Map<String, Object>)r.get("value");
            assertEquals(map("string", "value",
                    "int", 10,
                    "nested.anotherkey", "anotherValue",
                    "nested.nested.somekey", "someValue",
                    "nested.nested.somenumeric", 123), resultMap);
        });
    }
}
