package apoc.agg;

import apoc.util.TestUtil;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.graphdb.Entity;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static apoc.util.TestUtil.testCall;
import static apoc.util.Util.map;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.neo4j.internal.helpers.collection.Iterators.asSet;

public class GraphAggregationTest {

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    @BeforeClass public static void setUp() throws Exception {
        TestUtil.registerProcedure(db, apoc.agg.Graph.class);
        db.executeTransactionally("CREATE (a:A {id:'a'})-[:AB {id:'ab'}]->(b:B {id:'b'})-[:BC {id:'bc'}]->(c:C {id:'c'}),(a)-[:AC {id:'ac'}]->(c)");
    }

    @Test
    public void testGraph() throws Exception {
        Map<String, Entity> pcs = db.executeTransactionally("MATCH (n) RETURN n.id as id, n UNION ALL MATCH ()-[n]->() RETURN n.id as id, n", Collections.emptyMap(),
                result -> result.stream().collect(Collectors.toMap(row -> row.get("id").toString(), row -> (Entity) row.get("n"))));

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
                    Map<String, List<Entity>> graph = (Map<String, List<Entity>>) row.get("g");
                    assertEquals(asSet(pcs.get("a"),pcs.get("b"),pcs.get("c")), asSet(graph.get("nodes").iterator()));
                    assertEquals(asSet(pcs.get("ab"),pcs.get("ac")), asSet(graph.get("relationships").iterator()));
                });
        testCall(db, "MATCH p=()-->() RETURN apoc.agg.graph(p) as g",
                (row) -> {
                    Map<String, List<Entity>> graph = (Map<String, List<Entity>>) row.get("g");
                    assertEquals(asSet(pcs.get("a"),pcs.get("b"),pcs.get("c")), asSet(graph.get("nodes").iterator()));
                    assertEquals(asSet(pcs.get("ab"),pcs.get("ac"),pcs.get("bc")), asSet(graph.get("relationships").iterator()));
         });
    }

    public Consumer<Map<String, Object>> assertABAB(Map<String, Entity> pcs) {
        return (row) -> {
            Map<String, List<Entity>> graph = (Map<String, List<Entity>>) row.get("g");
            assertEquals(asSet(pcs.get("a"),pcs.get("b")), asSet(graph.get("nodes").iterator()));
            assertEquals(asList(pcs.get("ab")), graph.get("relationships"));
        };
    }
}
