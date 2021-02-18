package apoc.merge;

import apoc.util.MapUtil;
import apoc.util.TestUtil;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphdb.*;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.Map;

import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testResult;
import static org.junit.Assert.*;

public class MergeTest {

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();


    @Before
    public void setUp() throws Exception {
        TestUtil.registerProcedure(db, Merge.class);
    }

    @Test
    public void testMergeNode() throws Exception {
        testCall(db, "CALL apoc.merge.node(['Person','Bastard'],{ssid:'123'}, {name:'John'}) YIELD node RETURN node",
                (row) -> {
                    Node node = (Node) row.get("node");
                    assertEquals(true, node.hasLabel(Label.label("Person")));
                    assertEquals(true, node.hasLabel(Label.label("Bastard")));
                    assertEquals("John", node.getProperty("name"));
                    assertEquals("123", node.getProperty("ssid"));
                });
    }

    @Test
    public void testMergeNodeWithPreExisting() throws Exception {
        db.executeTransactionally("CREATE (p:Person{ssid:'123', name:'Jim'})");
        testCall(db, "CALL apoc.merge.node(['Person'],{ssid:'123'}, {name:'John'}) YIELD node RETURN node",
                (row) -> {
                    Node node = (Node) row.get("node");
                    assertEquals(true, node.hasLabel(Label.label("Person")));
                    assertEquals("Jim", node.getProperty("name"));
                    assertEquals("123", node.getProperty("ssid"));
                });

        testResult(db, "match (p:Person) return count(*) as c", result ->
                assertEquals(1, (long)(Iterators.single(result.columnAs("c"))))
        );
    }

    @Test
    public void testMergeRelationships() throws Exception {
        db.executeTransactionally("create (:Person{name:'Foo'}), (:Person{name:'Bar'})");

        testCall(db, "MERGE (s:Person{name:'Foo'}) MERGE (e:Person{name:'Bar'}) WITH s,e CALL apoc.merge.relationship(s, 'KNOWS', {rid:123}, {since:'Thu'}, e) YIELD rel RETURN rel",
                (row) -> {
                    Relationship rel = (Relationship) row.get("rel");
                    assertEquals("KNOWS", rel.getType().name());
                    assertEquals(123l, rel.getProperty("rid"));
                    assertEquals("Thu", rel.getProperty("since"));
                });

        testCall(db, "MERGE (s:Person{name:'Foo'}) MERGE (e:Person{name:'Bar'}) WITH s,e CALL apoc.merge.relationship(s, 'KNOWS', {rid:123}, {since:'Fri'}, e) YIELD rel RETURN rel",
                (row) -> {
                    Relationship rel = (Relationship) row.get("rel");
                    assertEquals("KNOWS", rel.getType().name());
                    assertEquals(123l, rel.getProperty("rid"));
                    assertEquals("Thu", rel.getProperty("since"));
                });
        testCall(db, "MERGE (s:Person{name:'Foo'}) MERGE (e:Person{name:'Bar'}) WITH s,e CALL apoc.merge.relationship(s, 'OTHER', null, null, e) YIELD rel RETURN rel",
                (row) -> {
                    Relationship rel = (Relationship) row.get("rel");
                    assertEquals("OTHER", rel.getType().name());
                    assertTrue(rel.getAllProperties().isEmpty());
                });
    }

    @Test
    public void testMergeWithEmptyIdentityPropertiesShouldFail() {
        for (String idProps: new String[]{"null", "{}"}) {
            try {
                testCall(db, "CALL apoc.merge.node(['Person']," + idProps +", {name:'John'}) YIELD node RETURN node",
                        row -> assertTrue(row.get("node") instanceof Node));
                fail();
            } catch (QueryExecutionException e) {
                assertTrue(e.getMessage().contains("you need to supply at least one identifying property for a merge"));
            }
        }
    }
    
    @Test
    public void testEscapeIdentityPropertiesWithSpecialCharactersShouldWork() {
        for (String key: new String[]{"normal", "i:d", "i-d", "i d"}) {
            Map<String, Object> identProps = MapUtil.map(key, "value");
            Map<String, Object> params = MapUtil.map("identProps", identProps);
            
            testCall(db, "CALL apoc.merge.node(['Person'], $identProps) YIELD node RETURN node", params,
                        (row) -> {
                            Node node = (Node) row.get("node");
                            assertTrue(node instanceof Node);
                            assertTrue(node.hasProperty(key));
                            assertEquals("value", node.getProperty(key));
                        });
        }
    }
    
    @Test
    public void testLabelsWithSpecialCharactersShouldWork() {
        for (String label: new String[]{"Label with spaces", ":LabelWithColon", "label-with-dash", "LabelWithUmlautsÄÖÜ"}) {
            Map<String, Object> params = MapUtil.map("label", label);
            testCall(db, "CALL apoc.merge.node([$label],{id:1}, {name:'John'}) YIELD node RETURN node", params,
                    row -> assertTrue(row.get("node") instanceof Node));
        }
    }

    @Test
    public void testRelationshipTypesWithSpecialCharactersShouldWork() {
        for (String relType: new String[]{"Reltype with space", ":ReltypeWithCOlon", "rel-type-with-dash"}) {
            Map<String, Object> params = MapUtil.map("relType", relType);
            testCall(db, "CREATE (a), (b) WITH a,b CALL apoc.merge.relationship(a, $relType, null, null, b) YIELD rel RETURN rel", params,
                    row -> assertTrue(row.get("rel") instanceof Relationship));
        }
    }


    // MERGE EAGER TESTS


    @Test
    public void testMergeEagerNode() throws Exception {
        testCall(db, "CALL apoc.merge.node.eager(['Person','Bastard'],{ssid:'123'}, {name:'John'}) YIELD node RETURN node",
                (row) -> {
                    Node node = (Node) row.get("node");
                    assertEquals(true, node.hasLabel(Label.label("Person")));
                    assertEquals(true, node.hasLabel(Label.label("Bastard")));
                    assertEquals("John", node.getProperty("name"));
                    assertEquals("123", node.getProperty("ssid"));
                });
    }

    @Test
    public void testMergeEagerNodeWithOnCreate() throws Exception {
        testCall(db, "CALL apoc.merge.node.eager(['Person','Bastard'],{ssid:'123'}, {name:'John'},{occupation:'juggler'}) YIELD node RETURN node",
                (row) -> {
                    Node node = (Node) row.get("node");
                    assertEquals(true, node.hasLabel(Label.label("Person")));
                    assertEquals(true, node.hasLabel(Label.label("Bastard")));
                    assertEquals("John", node.getProperty("name"));
                    assertEquals("123", node.getProperty("ssid"));
                    assertFalse(node.hasProperty("occupation"));
                });
    }

    @Test
    public void testMergeEagerNodeWithOnMatch() throws Exception {
        db.executeTransactionally("CREATE (p:Person:Bastard {ssid:'123'})");
        testCall(db, "CALL apoc.merge.node.eager(['Person','Bastard'],{ssid:'123'}, {name:'John'}, {occupation:'juggler'}) YIELD node RETURN node",
                (row) -> {
                    Node node = (Node) row.get("node");
                    assertEquals(true, node.hasLabel(Label.label("Person")));
                    assertEquals(true, node.hasLabel(Label.label("Bastard")));
                    assertEquals("juggler", node.getProperty("occupation"));
                    assertEquals("123", node.getProperty("ssid"));
                    assertFalse(node.hasProperty("name"));
                });
    }

    @Test
    public void testMergeEagerNodesWithOnMatchCanMergeOnMultipleMatches() throws Exception {
        db.executeTransactionally("UNWIND range(1,5) as index MERGE (:Person:`Bastard Man`{ssid:'123', index:index})");

        try (Transaction tx = db.beginTx()) {
            Result result = tx.execute("CALL apoc.merge.node.eager(['Person','Bastard Man'],{ssid:'123'}, {name:'John'}, {occupation:'juggler'}) YIELD node RETURN node");

            for (long index = 1; index <= 5; index++) {
                Node node = (Node) result.next().get("node");
                assertEquals(true, node.hasLabel(Label.label("Person")));
                assertEquals(true, node.hasLabel(Label.label("Bastard Man")));
                assertEquals("123", node.getProperty("ssid"));
                assertEquals(index, node.getProperty("index"));
                assertEquals("juggler", node.getProperty("occupation"));
                assertFalse(node.hasProperty("name"));
            }
        } catch (Exception e) {
            fail();
        }
    }

    @Test
    public void testMergeEagerRelationships() throws Exception {
        db.executeTransactionally("create (:Person{name:'Foo'}), (:Person{name:'Bar'})");

        testCall(db, "MERGE (s:Person{name:'Foo'}) MERGE (e:Person{name:'Bar'}) WITH s,e CALL apoc.merge.relationship.eager(s, 'KNOWS', {rid:123}, {since:'Thu'}, e) YIELD rel RETURN rel",
                (row) -> {
                    Relationship rel = (Relationship) row.get("rel");
                    assertEquals("KNOWS", rel.getType().name());
                    assertEquals(123l, rel.getProperty("rid"));
                    assertEquals("Thu", rel.getProperty("since"));
                });

        testCall(db, "MERGE (s:Person{name:'Foo'}) MERGE (e:Person{name:'Bar'}) WITH s,e CALL apoc.merge.relationship.eager(s, 'KNOWS', {rid:123}, {since:'Fri'}, e) YIELD rel RETURN rel",
                (row) -> {
                    Relationship rel = (Relationship) row.get("rel");
                    assertEquals("KNOWS", rel.getType().name());
                    assertEquals(123l, rel.getProperty("rid"));
                    assertEquals("Thu", rel.getProperty("since"));
                });
        testCall(db, "MERGE (s:Person{name:'Foo'}) MERGE (e:Person{name:'Bar'}) WITH s,e CALL apoc.merge.relationship(s, 'OTHER', null, null, e) YIELD rel RETURN rel",
                (row) -> {
                    Relationship rel = (Relationship) row.get("rel");
                    assertEquals("OTHER", rel.getType().name());
                    assertTrue(rel.getAllProperties().isEmpty());
                });
    }

    @Test
    public void testMergeEagerRelationshipsWithOnMatch() throws Exception {
        db.executeTransactionally("create (:Person{name:'Foo'}), (:Person{name:'Bar'})");

        testCall(db, "MERGE (s:Person{name:'Foo'}) MERGE (e:Person{name:'Bar'}) WITH s,e CALL apoc.merge.relationship.eager(s, 'KNOWS', {rid:123}, {since:'Thu'}, e,{until:'Saturday'}) YIELD rel RETURN rel",
                (row) -> {
                    Relationship rel = (Relationship) row.get("rel");
                    assertEquals("KNOWS", rel.getType().name());
                    assertEquals(123l, rel.getProperty("rid"));
                    assertEquals("Thu", rel.getProperty("since"));
                    assertFalse(rel.hasProperty("until"));
                });

        testCall(db, "MERGE (s:Person{name:'Foo'}) MERGE (e:Person{name:'Bar'}) WITH s,e CALL apoc.merge.relationship.eager(s, 'KNOWS', {rid:123}, {}, e,{since:'Fri'}) YIELD rel RETURN rel",
                (row) -> {
                    Relationship rel = (Relationship) row.get("rel");
                    assertEquals("KNOWS", rel.getType().name());
                    assertEquals(123l, rel.getProperty("rid"));
                    assertEquals("Fri", rel.getProperty("since"));
                });
    }

    @Test
    public void testMergeEagerRelationshipsWithOnMatchCanMergeOnMultipleMatches() throws Exception {
        db.executeTransactionally("CREATE (foo:Person{name:'Foo'}), (bar:Person{name:'Bar'}) WITH foo, bar UNWIND range(1,3) as index CREATE (foo)-[:KNOWS {rid:123}]->(bar)");

        try (Transaction tx = db.beginTx()) {
            Result result = tx.execute("MERGE (s:Person{name:'Foo'}) MERGE (e:Person{name:'Bar'}) WITH s,e CALL apoc.merge.relationship.eager(s, 'KNOWS', {rid:123}, {}, e, {since:'Fri'}) YIELD rel RETURN rel");

            for (long index = 1; index <= 3; index++) {
                Relationship rel = (Relationship) result.next().get("rel");
                assertEquals("KNOWS", rel.getType().name());
                assertEquals(123l, rel.getProperty("rid"));
                assertEquals("Fri", rel.getProperty("since"));
            }
        } catch (Exception e) {
            fail();
        }
    }

    @Test
    public void testMergeEagerWithEmptyIdentityPropertiesShouldFail() {
        for (String idProps: new String[]{"null", "{}"}) {
            try {
                testCall(db, "CALL apoc.merge.node(['Person']," + idProps +", {name:'John'}) YIELD node RETURN node",
                        row -> assertTrue(row.get("node") instanceof Node));
                fail();
            } catch (QueryExecutionException e) {
                assertTrue(e.getMessage().contains("you need to supply at least one identifying property for a merge"));
            }
        }
    }
}
