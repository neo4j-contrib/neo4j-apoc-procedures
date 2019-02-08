package apoc.graph;

import apoc.result.VirtualNode;
import apoc.result.VirtualRelationship;
import apoc.util.TestUtil;
import apoc.util.Util;
import junit.framework.AssertionFailedError;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.*;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static apoc.util.MapUtil.map;
import static java.util.Arrays.asList;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.neo4j.graphdb.Label.label;

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

    @Test
    public void testInverseToTree() {
        String json = "{'id': 1, "
                + "'type': 'artist',"
                + "'name': 'Genesis',"
                + "'albums': ["
                + "             {"
                + "               'type': 'album',"
                + "               'id': 1,"
                + "               'producer': 'Jonathan King',"
                + "               'title': 'From Genesis to Revelation'"
                + "             }"
                + "]"
                + "}";

        TestUtil.testResult(db, "CALL apoc.graph.inverseToTree({json}, true) yield graph", Util.map("json", json), stringObjectMap -> {
            Map<String, Object> map = stringObjectMap.next();
            assertEquals("Graph", ((Map) map.get("graph")).get("name"));
            List<Node> virtualNodeList = (List<Node>) ((Map) map.get("graph")).get("nodes");
            Map<String, Object> node1 = new HashMap<>();
            node1.put("producer", "Jonathan King");
            node1.put("id", 1L);
            node1.put("type", "album");
            node1.put("title", "From Genesis to Revelation");
            assertEquals(asList(label("Album")), virtualNodeList.get(0).getLabels());
            assertTrue(virtualNodeList.get(0).getId() > 0);
            assertEquals(node1, virtualNodeList.get(0).getAllProperties());
            Map<String, Object> node2 = new HashMap<>();
            node2.put("name", "Genesis");
            node2.put("id", 1L);
            node2.put("type", "artist");
            assertEquals(asList(label("Artist")), virtualNodeList.get(1).getLabels());
            assertTrue(virtualNodeList.get(1).getId() > 0);
            assertEquals(node2, virtualNodeList.get(1).getAllProperties());
            List<Relationship> virtualRelationshipList = (List<Relationship>) ((Map) map.get("graph")).get("relationships");
            assertEquals("ALBUM", virtualRelationshipList.get(0).getType().name());
            assertTrue(virtualRelationshipList.get(0).getId() > 0);
        });

        db.execute("MATCH p=(a:Artist)-[r:ALBUM]->(b:Album) detach delete (p)").close();

    }

    @Test
    public void testInverseToTreeVirtual() {
        String json = "{'id': 1, "
                + "'type': 'artist',"
                + "'name': 'Genesis',"
                + "'albums': ["
                + "             {"
                + "               'type': 'album',"
                + "               'id': 1,"
                + "               'producer': 'Jonathan King',"
                + "               'title': 'From Genesis to Revelation'"
                + "             }"
                + "            ]"
                + "}";

        TestUtil.testResult(db, "CALL apoc.graph.inverseToTree({json}) yield graph", Util.map("json", json), stringObjectMap -> {
            Map<String, Object> map = stringObjectMap.next();
            assertEquals("Graph", ((Map) map.get("graph")).get("name"));
            List<VirtualNode> virtualNodeList = (List<VirtualNode>) ((Map) map.get("graph")).get("nodes");
            Map<String, Object> node1 = new HashMap<>();
            node1.put("producer", "Jonathan King");
            node1.put("id", 1L);
            node1.put("type", "album");
            node1.put("title", "From Genesis to Revelation");
            assertEquals(asList(label("Album")), virtualNodeList.get(0).getLabels());
            assertTrue(virtualNodeList.get(0).getId() < 0);
            assertEquals(node1, virtualNodeList.get(0).getAllProperties());
            Map<String, Object> node2 = new HashMap<>();
            node2.put("name", "Genesis");
            node2.put("id", 1L);
            node2.put("type", "artist");
            assertEquals(asList(label("Artist")), virtualNodeList.get(1).getLabels());
            assertTrue(virtualNodeList.get(1).getId() < 0);
            assertEquals(node2, virtualNodeList.get(1).getAllProperties());
            List<VirtualRelationship> virtualRelationshipList = (List<VirtualRelationship>) ((Map) map.get("graph")).get("relationships");
            assertEquals(RelationshipType.withName("ALBUM"), virtualRelationshipList.get(0).getType());
            assertTrue(virtualRelationshipList.get(0).getId() < 0);
        });
    }

    @Test(expected = RuntimeException.class)
    public void testCreateVirtualSimpleNodeWithErrorId() throws Exception{
        String json = "{"//missing ID
                + "'type': 'artist',"
                + "'name': 'Genesis'"
                + "}";

        try {
            TestUtil.testCall(db, "CALL apoc.graph.inverseToTree({json}) yield graph", Util.map("json", json), stringObjectMap -> { });
        } catch (QueryExecutionException e) {
            Throwable except = ExceptionUtils.getRootCause(e);
            assertTrue(except instanceof RuntimeException);
            assertEquals("every object must have id: {name=Genesis, type=artist}", except.getMessage());
            throw e;
        }
    }

    @Test(expected = RuntimeException.class)
    public void testCreateVirtualSimpleNodeWithErrorType() throws Exception{
        String json = "{"//missing ID
                + "'id': 1,"
                + "'name': 'Genesis'"
                + "}";

        try {
            TestUtil.testCall(db, "CALL apoc.graph.inverseToTree({json}) yield graph", Util.map("json", json), stringObjectMap -> { });
        } catch (QueryExecutionException e) {
            Throwable except = ExceptionUtils.getRootCause(e);
            assertTrue(except instanceof RuntimeException);
            assertEquals("every object must have type: {name=Genesis, id=1}", except.getMessage());
            throw e;
        }
    }

    @Test
    public void testCreateVirtualSimpleNode() throws Exception{
        String json = "{"
                + "'id': 1,"
                + "'type': 'artist',"
                + "'name': 'Genesis'"
                + "}";

            TestUtil.testResult(db, "CALL apoc.graph.inverseToTree({json}) yield graph", Util.map("json", json), stringObjectMap -> {
                Map<String, Object> map = stringObjectMap.next();
                assertEquals("Graph", ((Map) map.get("graph")).get("name"));
                List<VirtualNode> virtualNodeList = (List<VirtualNode>) ((Map) map.get("graph")).get("nodes");
                Map<String, Object> node1 = new HashMap<>();
                node1.put("id", 1L);
                node1.put("name", "Genesis");
                node1.put("type", "artist");
                assertEquals(asList(label("Artist")), virtualNodeList.get(0).getLabels());
                assertTrue(virtualNodeList.get(0).getId() < 0);
                assertEquals(node1, virtualNodeList.get(0).getAllProperties());
            });
    }

    @Test
    public void testCreateVirtualNodeArray() {
        String json = "{'id': 1, "
                + "'type': 'artist',"
                + "'name': 'Genesis',"
                + "'albums': ["
                + "             {"
                + "               'type': 'album',"
                + "               'id': 1,"
                + "               'producer': 'Jonathan King',"
                + "               'title': 'From Genesis to Revelation'"
                + "             },"
                + "             {"
                + "               'type': 'album',"
                + "               'id': 2,"
                + "               'producer': 'Jonathan King',"
                + "               'title': 'John Anthony'"
                + "             }"
                + "]"
                + "}";

        TestUtil.testResult(db, "CALL apoc.graph.inverseToTree({json}) yield graph", Util.map("json", json), stringObjectMap -> {
            Map<String, Object> map = stringObjectMap.next();
            assertEquals("Graph", ((Map) map.get("graph")).get("name"));
            List<VirtualNode> virtualNodeList = (List<VirtualNode>) ((Map) map.get("graph")).get("nodes");
            Map<String, Object> node1 = new HashMap<>();
            node1.put("producer", "Jonathan King");
            node1.put("id", 1L);
            node1.put("type", "album");
            node1.put("title", "From Genesis to Revelation");
            assertEquals(asList(label("Album")), virtualNodeList.get(0).getLabels());
            assertTrue(virtualNodeList.get(0).getId() < 0);
            assertEquals(node1, virtualNodeList.get(0).getAllProperties());
            Map<String, Object> node2 = new HashMap<>();
            node2.put("producer", "Jonathan King");
            node2.put("id", 2L);
            node2.put("title", "John Anthony");
            node2.put("type", "album");
            assertEquals(asList(label("Album")), virtualNodeList.get(1).getLabels());
            assertTrue(virtualNodeList.get(1).getId() < 0);
            assertEquals(node2, virtualNodeList.get(1).getAllProperties());
            Map<String, Object> node3 = new HashMap<>();
            node3.put("name", "Genesis");
            node3.put("id", 1L);
            node3.put("type", "artist");
            assertEquals(asList(label("Artist")), virtualNodeList.get(2).getLabels());
            assertTrue(virtualNodeList.get(2).getId() < 0);
            assertEquals(node3, virtualNodeList.get(2).getAllProperties());
            List<VirtualRelationship> virtualRelationshipList = (List<VirtualRelationship>) ((Map) map.get("graph")).get("relationships");
            assertEquals(RelationshipType.withName("ALBUM"), virtualRelationshipList.get(0).getType());
            assertTrue(virtualRelationshipList.get(0).getId() < 0);
            assertEquals(RelationshipType.withName("ALBUM"), virtualRelationshipList.get(1).getType());
            assertTrue(virtualRelationshipList.get(1).getId() < 0);
        });
    }

    @Test
    public void shouldCreatePrimitiveArray() {
        String json = "{'id': 1, "
                + "'type': 'artist',"
                + "'name': 'Genesis',"
                + "'members': ['Tony Banks','Mike Rutherford','Phil Collins'],"
                + "'years': [1967, 1998, 1999, 2000, 2006]"
                + "}";

        TestUtil.testResult(db, "CALL apoc.graph.inverseToTree({json}) yield graph", Util.map("json", json), stringObjectMap -> {
            Map<String, Object> map = stringObjectMap.next();
            assertEquals("Graph", ((Map) map.get("graph")).get("name"));
            List<VirtualNode> virtualNodeList = (List<VirtualNode>) ((Map) map.get("graph")).get("nodes");
            assertEquals(asList(label("Artist")), virtualNodeList.get(0).getLabels());
            assertTrue(virtualNodeList.get(0).getId() < 0);

            assertEquals(1L,virtualNodeList.get(0).getProperty("id"));
            assertEquals("Genesis",virtualNodeList.get(0).getProperty("name"));
            assertEquals("artist",virtualNodeList.get(0).getProperty("type"));
            Arrays.equals(new String[]{"1967", "1998", "1999", "2000", "2006"}, (Object[]) virtualNodeList.get(0).getProperty("years"));
            Arrays.equals(new String[]{"Tony Banks","Mike Rutherford","Phil Collins"}, (Object[]) virtualNodeList.get(0).getProperty("members"));
        });
    }

}
