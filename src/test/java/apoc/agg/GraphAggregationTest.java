package apoc.agg;

import apoc.util.TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static apoc.util.TestUtil.testCall;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.junit.Assert.assertEquals;
import static apoc.util.Util.map;
import static org.neo4j.helpers.collection.Iterators.asSet;

public class GraphAggregationTest {

    private static GraphDatabaseService db;

    @BeforeClass public static void setUp() throws Exception {
        db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        TestUtil.registerProcedure(db, apoc.agg.Graph.class);
        db.execute("CREATE (a:A {id:'a'})-[:AB {id:'ab'}]->(b:B {id:'b'})-[:BC {id:'bc'}]->(c:C {id:'c'}),(a)-[:AC {id:'ac'}]->(c)").close();
    }

    @AfterClass public static void tearDown() {
        db.shutdown();
    }

    @Test
    public void testGraph() throws Exception {
        Map<String, PropertyContainer> pcs = new HashMap<>();
        db.execute("MATCH (n) RETURN n.id as id, n UNION ALL MATCH ()-[n]->() RETURN n.id as id, n")
                .stream().forEach(row -> pcs.put(row.get("id").toString(), (PropertyContainer)row.get("n")));

        testCall(db, "RETURN apoc.agg.graph(null) as g",
                (row) -> {
                    assertEquals(map("nodes", asList(),
                            "relationships",asList()), row.get("g"));
                });
        testCall(db, "MATCH p=(:A)-->(:B) RETURN apoc.agg.graph(p) as g", assertABAB(pcs));
        testCall(db, "MATCH p=(:A)-->(:B) RETURN apoc.agg.graph([p]) as g", assertABAB(pcs));
        testCall(db, "MATCH p=(:A)-[r]->(:B) RETURN apoc.agg.graph(r) as g", assertABAB(pcs));
        testCall(db, "MATCH p=(:A)-[r]->(:B) RETURN apoc.agg.graph([r]) as g", assertABAB(pcs));
        testCall(db, "MATCH p=(:A)-[r]->(:B) RETURN apoc.agg.graph({r:r}) as g", assertABAB(pcs));
        testCall(db, "MATCH p=(:A)-[r]->(:B) RETURN apoc.agg.graph({r:[r]}) as g", assertABAB(pcs));
        testCall(db, "MATCH p=(a:A)-[r]->(b:B) UNWIND [a,b,r] as e RETURN apoc.agg.graph(e) as g", assertABAB(pcs));
        testCall(db, "MATCH p=(a:A)-[r]->(b:B) UNWIND [a,b,r] as e RETURN apoc.agg.graph([e]) as g", assertABAB(pcs));
        testCall(db, "MATCH p=(a:A)-[r]->(b:B) UNWIND [a,b,r] as e RETURN apoc.agg.graph({e:e}) as g", assertABAB(pcs));
        testCall(db, "MATCH p=(a:A)-[r]->(b:B) UNWIND [a,b,r] as e RETURN apoc.agg.graph({e:[e]}) as g", assertABAB(pcs));
        testCall(db, "MATCH p=(a:A)-[r]->(b:B) RETURN apoc.agg.graph([a,b,r]) as g", assertABAB(pcs));
        testCall(db, "MATCH p=(a:A)-[r]->(b:B) RETURN apoc.agg.graph({a:a,b:b,r:r}) as g", assertABAB(pcs));
        testCall(db, "MATCH p=(a:A)-[r]->(b:B) RETURN apoc.agg.graph([{a:a,b:b,r:r}]) as g", assertABAB(pcs));
        testCall(db, "MATCH p=(:A)-->() RETURN apoc.agg.graph(p) as g",
                (row) -> {
                    Map<String, List<PropertyContainer>> graph = (Map<String, List<PropertyContainer>>) row.get("g");
                    assertEquals(asSet(pcs.get("a"),pcs.get("b"),pcs.get("c")), asSet(graph.get("nodes").iterator()));
                    assertEquals(asSet(pcs.get("ab"),pcs.get("ac")), asSet(graph.get("relationships").iterator()));
                });
        testCall(db, "MATCH p=()-->() RETURN apoc.agg.graph(p) as g",
                (row) -> {
                    Map<String, List<PropertyContainer>> graph = (Map<String, List<PropertyContainer>>) row.get("g");
                    assertEquals(asSet(pcs.get("a"),pcs.get("b"),pcs.get("c")), asSet(graph.get("nodes").iterator()));
                    assertEquals(asSet(pcs.get("ab"),pcs.get("ac"),pcs.get("bc")), asSet(graph.get("relationships").iterator()));
         });
    }

    public Consumer<Map<String, Object>> assertABAB(Map<String, PropertyContainer> pcs) {
        return (row) -> {
            Map<String, List<PropertyContainer>> graph = (Map<String, List<PropertyContainer>>) row.get("g");
            assertEquals(asSet(pcs.get("a"),pcs.get("b")), asSet(graph.get("nodes").iterator()));
            assertEquals(asList(pcs.get("ab")), graph.get("relationships"));
        };
    }
}
