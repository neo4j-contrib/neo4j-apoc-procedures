package apoc.graph;

import apoc.util.TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.HashMap;
import java.util.Map;

import static apoc.util.MapUtil.map;
import static org.junit.Assert.assertEquals;

/**
 * @author mh
 * @since 27.05.16
 */
public class GraphsTest {

    private static GraphDatabaseService db;

    private static Map<String,Object> graph = map("name","test","properties",map("answer",42L));
    @BeforeClass
    public static void setUp() throws Exception {
        db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        TestUtil.registerProcedure(db,Graphs.class);
        Result result = db.execute("CREATE (a:Actor {name:'Tom Hanks'})-[r:ACTED_IN {roles:'Forrest'}]->(m:Movie {title:'Forrest Gump'}) RETURN [a,m] as nodes, [r] as relationships");
        while (result.hasNext()) {
            graph.putAll(result.next());
        }

    }
    @AfterClass
    public static void tearDown() {
        db.shutdown();
    }

    @Test
    public void testFromData() throws Exception {
        TestUtil.testCall(db,"MATCH (n)-[r]->(m) CALL apoc.graph.fromData([n,m],[r],'test',{answer:42}) YIELD graph RETURN *",
                r -> assertEquals(graph, r.get("graph")));
    }

    @Test
    public void testFromPath() throws Exception {
        TestUtil.testCall(db,"MATCH path = (n)-[r]->(m) CALL apoc.graph.fromPath(path,'test',{answer:42}) YIELD graph RETURN *",
                r -> assertEquals(graph, r.get("graph")));
    }

    @Test
    public void testFromPaths() throws Exception {
        TestUtil.testCall(db,"MATCH path = (n)-[r]->(m) CALL apoc.graph.fromPaths([path],'test',{answer:42}) YIELD graph RETURN *",
                r -> assertEquals(graph, r.get("graph")));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testFromDB() throws Exception {
        TestUtil.testCall(db," CALL apoc.graph.fromDB('test',{answer:42})",
                r -> {
                    Map graph1 = new HashMap((Map)r.get("graph"));
                    graph1.put("nodes", Iterables.asList((Iterable) graph1.get("nodes")));
                    graph1.put("relationships", Iterables.asList((Iterable) graph1.get("relationships")));
                    assertEquals(graph, graph1);
                });
    }

    @Test
    public void testFromCypher() throws Exception {
        TestUtil.testCall(db,"CALL apoc.graph.fromCypher('MATCH (n)-[r]->(m) RETURN *',null,'test',{answer:42}) YIELD graph RETURN *",
                r -> assertEquals(graph, r.get("graph")));
    }
}
