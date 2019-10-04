package apoc.refactor;

import apoc.util.ArrayBackedList;
import apoc.util.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphdb.*;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.*;
import static java.util.Arrays.asList;
import static java.util.Collections.*;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.*;

/**
 * @author mh
 * @since 25.03.16
 */
public class GraphRefactoringTest {

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();

    @Before
    public void setUp() throws Exception {
        TestUtil.registerProcedure(db, GraphRefactoring.class);
    }

    @After
    public void tearDown() {
        db.shutdown();
    }

    @Test
    public void testDeleteOneNode() throws Exception {
        long id = db.executeTransactionally("CREATE (p1:Person {ID:1}), (p2:Person {ID:2}) RETURN id(p1) as id ", emptyMap(), result -> Iterators.single(result.columnAs("id")));
        testCall(db, "MATCH (o:Person {ID:$oldID}), (n:Person {ID:$newID}) DELETE o RETURN n as node",
                      map("oldID", 1L, "newID",2L),
                (r) -> {
                    Node node = (Node) r.get("node");
                    assertNotEquals(id, node.getId());
                    assertEquals(true, node.hasLabel(Label.label("Person")));
                    assertEquals(2L, node.getProperty("ID"));
                });
    }

    @Test
    public void testEagernessMergeNodesFails() throws Exception {
        db.executeTransactionally("CREATE INDEX ON :Person(ID)");
        long id = db.executeTransactionally("CREATE (p1:Person {ID:1}), (p2:Person {ID:2}) RETURN id(p1) as id ", emptyMap(), result -> Iterators.single(result.columnAs("id")));
        testCall(db, "MATCH (o:Person {ID:$oldID}), (n:Person {ID:$newID}) CALL apoc.refactor.mergeNodes([o,n]) yield node return node",
                      map("oldID", 1L, "newID",2L),
                (r) -> {
                    Node node = (Node) r.get("node");
                    assertEquals(id, node.getId());
                    assertEquals(true, node.hasLabel(Label.label("Person")));
                    assertEquals(2L, node.getProperty("ID"));
                });
    }

    @Test
    public void testMergeNodesEagerAggregation() throws Exception {
        long id = db.executeTransactionally("CREATE (p1:Person {ID:1}), (p2:Person {ID:2}) RETURN id(p1) as id ", emptyMap(), result -> Iterators.single(result.columnAs("id")));
        testCall(db, "MATCH (o:Person {ID:$oldID}), (n:Person {ID:$newID}) WITH head(collect([o,n])) as nodes CALL apoc.refactor.mergeNodes(nodes) yield node return node",
                      map("oldID", 1L, "newID",2L),
                (r) -> {
                    Node node = (Node) r.get("node");
                    assertEquals(id, node.getId());
                    assertEquals(true, node.hasLabel(Label.label("Person")));
                    assertEquals(2L, node.getProperty("ID"));
                });
    }

    @Test
    public void testMergeNodesEagerIndex() throws Exception {
        db.executeTransactionally("CREATE INDEX ON :Person(ID)");
        db.executeTransactionally("CALL db.awaitIndexes()");
        long id = db.executeTransactionally("CREATE (p1:Person {ID:1}), (p2:Person {ID:2}) RETURN id(p1) as id ", emptyMap(), result -> Iterators.single(result.columnAs("id")));
        testCall(db, "MATCH (o:Person {ID:$oldID}), (n:Person {ID:$newID}) USING INDEX o:Person(ID) USING INDEX n:Person(ID) CALL apoc.refactor.mergeNodes([o,n]) yield node return node",
                      map("oldID", 1L, "newID",2L),
                (r) -> {
                    Node node = (Node) r.get("node");
                    assertEquals(id, node.getId());
                    assertEquals(true, node.hasLabel(Label.label("Person")));
                    assertEquals(2L, node.getProperty("ID"));
                });
    }
    @Test
    public void testMergeNodesIndexConflict() throws Exception {
        /*
        CREATE CONSTRAINT ON (a:A) ASSERT a.prop1 IS UNIQUE;
CREATE CONSTRAINT ON (a:B) ASSERT a.prop2 IS UNIQUE;
CREATE (a:A) SET a.prop1 = 1;
CREATE (b:B) SET b.prop2 = 99;

MATCH (a:A {prop1:1}) MATCH (b:B {prop2:99}) CALL apoc.refactor.mergeNodes([a, b]) YIELD node RETURN node;
         */
        db.executeTransactionally("CREATE CONSTRAINT ON (a:A) ASSERT a.prop1 IS UNIQUE;");
        db.executeTransactionally("CREATE CONSTRAINT ON (b:B) ASSERT b.prop2 IS UNIQUE;");
        db.executeTransactionally("CALL db.awaitIndexes()");
        long id = db.executeTransactionally("CREATE (a:A) SET a.prop1 = 1 CREATE (b:B) SET b.prop2 = 99 RETURN id(a) as id ", emptyMap(), result -> Iterators.single(result.columnAs("id")));
        testCall(db, "MATCH (a:A {prop1:1}) MATCH (b:B {prop2:99}) CALL apoc.refactor.mergeNodes([a, b]) YIELD node RETURN node",
                      map("oldID", 1L, "newID",2L),
                (r) -> {
                    Node node = (Node) r.get("node");
                    assertEquals(id, node.getId());
                    assertTrue(node.hasLabel(Label.label("A")));
                    assertTrue(node.hasLabel(Label.label("B")));
                    assertEquals(1L, node.getProperty("prop1"));
                    assertEquals(99L, node.getProperty("prop2"));
                });
    }

    /*
    ISSUE #590
     */
    @Test
    public void testMergeMultipleNodesRelationshipDirection() {
        db.executeTransactionally("create (a1:ALabel {name:'a1'})-[:HAS_REL]->(b1:BLabel {name:'b1'})," +
                "          (a2:ALabel {name:'a2'})-[:HAS_REL]->(b2:BLabel {name:'b2'})," +
                "          (a3:ALabel {name:'a3'})-[:HAS_REL]->(b3:BLabel {name:'b3'}), " +
                "          (a4:ALabel {name:'a4'})-[:HAS_REL]->(b4:BLabel {name:'b4'})");

        testCall(db, "MATCH (b1:BLabel {name:'b1'}), (b2:BLabel {name:'b2'}), (b3:BLabel {name:'b3'}), (b4:BLabel {name:'b4'}) " +
                "     WITH head(collect([b1,b2,b3,b4])) as nodes CALL apoc.refactor.mergeNodes(nodes) yield node return node",
                row -> {
                    assertTrue(row.get("node") != null);
                    assertTrue(row.get("node") instanceof Node);
                    Node resultingNode = (Node)(row.get("node"));
                    assertTrue(resultingNode.getDegree(Direction.OUTGOING) == 0);
                    assertTrue(resultingNode.getDegree(Direction.INCOMING) == 4);
                }
        );
    }

    @Test
    public void testMergeNodesWithNonDistinct() {
        db.executeTransactionally("create (a1:ALabel {name:'a1'})-[:HAS_REL]->(b1:BLabel {name:'b1'})," +
                "          (a2:ALabel {name:'a2'})-[:HAS_REL]->(b2:BLabel {name:'b2'})," +
                "          (a3:ALabel {name:'a3'})-[:HAS_REL]->(b3:BLabel {name:'b3'}) ");

        testCall(db, "MATCH (a1:ALabel{name:'a1'}),(a2:ALabel{name:'a2'}),(a3:ALabel{name:'a3'}) " +
                //                 | here we're using a2 two times!
                //                \/
                        "WITH [a1,a2,a2,a3] as nodes limit 1 " +
                        "CALL apoc.refactor.mergeNodes(nodes) yield node return node",
                row -> {
                    Node node = (Node) row.get("node");
                    assertNotNull(node);
                    assertTrue(node.getDegree(Direction.OUTGOING) == 3);
                    assertTrue(node.getDegree(Direction.INCOMING) == 0);
                }
        );

        testResult(db, "MATCH (a:ALabel) return count(*) as count", result -> {
            assertEquals( "other ALabel nodes have been deleted", 1, (long)Iterators.single(result.columnAs("count")));
        });
    }

    @Test
    public void testMergeNodesOneSingleNode() {
        db.executeTransactionally("create (a1:ALabel {name:'a1'})-[:HAS_REL]->(b1:BLabel {name:'b1'})");
        testCall(db, "MATCH (a1:ALabel{name:'a1'}) " +
                        "WITH a1 limit 1 " +
                        "CALL apoc.refactor.mergeNodes([a1]) yield node return node",
                row -> {
                    Node node = (Node) row.get("node");
                    assertNotNull(node);
                    assertTrue(node.getDegree(Direction.OUTGOING) == 1);
                    assertTrue(node.getDegree(Direction.INCOMING) == 0);
                }
        );
    }

    @Test
    public void testMergeNodesIsTolerantForDeletedNodes() {
        db.executeTransactionally("create (a1:ALabel {name:'a1'})-[:HAS_REL]->(b1:BLabel {name:'b1'})," +
                "(a2:ALabel {name:'a2'}), " +
                "(a3:ALabel {name:'a3'})-[:HAS_REL]->(b1)");
        testCall(db, "MATCH (a1:ALabel{name:'a1'}), (a2:ALabel{name:'a2'}), (a3:ALabel{name:'a3'}) " +
                        "WITH a1,a2,a3 limit 1 " +
                        "DELETE a2 " +
                        "WITH a1, a2, a3 " +
                        "CALL apoc.refactor.mergeNodes([a1,a2,a3]) yield node return node",
                row -> {
                    Node node = (Node) row.get("node");
                    assertNotNull(node);
                    assertTrue(node.getDegree(Direction.OUTGOING) == 2);
                    assertTrue(node.getDegree(Direction.INCOMING) == 0);
                }
        );
    }

    @Test
    public void testExtractNode() throws Exception {
        Long id = db.executeTransactionally("CREATE (f:Foo)-[rel:FOOBAR {a:1}]->(b:Bar) RETURN id(rel) as id", emptyMap(), result -> Iterators.single(result.columnAs("id")));
        testCall(db, "CALL apoc.refactor.extractNode($ids,['FooBar'],'FOO','BAR')", map("ids", singletonList(id)),
                (r) -> {
                    assertEquals(id, r.get("input"));
                    Node node = (Node) r.get("output");
                    assertEquals(true, node.hasLabel(Label.label("FooBar")));
                    assertEquals(1L, node.getProperty("a"));
                    assertNotNull(node.getSingleRelationship(RelationshipType.withName("FOO"), Direction.OUTGOING));
                    assertNotNull(node.getSingleRelationship(RelationshipType.withName("BAR"), Direction.INCOMING));
                });
    }
    @Test
    public void testInvertRelationship() throws Exception {
        long id = db.executeTransactionally("CREATE (f:Foo)-[rel:FOOBAR {a:1}]->(b:Bar) RETURN id(rel) as id", emptyMap(), result -> Iterators.single(result.columnAs("id")));
        testCall(db, "MATCH ()-[r]->() WHERE id(r) = $id CALL apoc.refactor.invert(r) yield input, output RETURN *", map("id", id),
                (r) -> {
                    assertEquals(id, r.get("input"));
                    Relationship rel = (Relationship) r.get("output");
                    assertEquals(true, rel.getStartNode().hasLabel(Label.label("Bar")));
                    assertEquals(true, rel.getEndNode().hasLabel(Label.label("Foo")));
                    assertEquals(1L, rel.getProperty("a"));
                });
    }

    @Test
    public void testCollapseNode() throws Exception {
        Long id = db.executeTransactionally("CREATE (f:Foo)-[:FOO {a:1}]->(b:Bar {c:3})-[:BAR {b:2}]->(f) RETURN id(b) as id", emptyMap(), result -> Iterators.single(result.columnAs("id")));
        testCall(db, "CALL apoc.refactor.collapseNode($ids,'FOOBAR')", map("ids", singletonList(id)),
                (r) -> {
                    assertEquals(id, r.get("input"));
                    Relationship rel = (Relationship) r.get("output");
                    assertEquals(true, rel.isType(RelationshipType.withName("FOOBAR")));
                    assertEquals(1L, rel.getProperty("a"));
                    assertEquals(2L, rel.getProperty("b"));
                    assertEquals(3L, rel.getProperty("c"));
                    assertNotNull(rel.getEndNode().hasLabel(Label.label("Foo")));
                    assertNotNull(rel.getStartNode().hasLabel(Label.label("Foo")));
                });
    }

    @Test
    public void testNormalizeAsBoolean() throws Exception {
        db.executeTransactionally("CREATE ({prop: 'Y', id:1}),({prop: 'Yes', id: 2}),({prop: 'NO', id: 3}),({prop: 'X', id: 4})");

        testResult(
            db,
            "MATCH (n) CALL apoc.refactor.normalizeAsBoolean(n,'prop',['Y','Yes'],['NO']) WITH n ORDER BY n.id RETURN n.prop AS prop",
            (r) -> {
                List<Boolean> result = new ArrayList<>();
                while (r.hasNext())
                    result.add((Boolean) r.next().get("prop"));
                assertThat(result, equalTo(Arrays.asList(true, true, false, null)));
            }
        );
    }

    private void categorizeWithDirection(Direction direction) {
        db.executeTransactionally(
                "CREATE ({prop: 'A', k: 'a', id: 1}) " +
                        "CREATE ({prop: 'A', k: 'a', id: 2}) " +
                        "CREATE ({prop: 'C', k: 'c', id: 3}) " +
                        "CREATE ({                   id: 4}) " +
                        "CREATE ({prop: 'B', k: 'b', id: 5}) " +
                        "CREATE ({prop: 'C', k: 'c', id: 6})");


        final boolean outgoing = direction == Direction.OUTGOING ? true : false;
        testCallEmpty(
                db,
                "CALL apoc.refactor.categorize('prop','IS_A', $direction, 'Letter','name',['k'],1)",
                map("direction", outgoing)
        );

        String traversePattern = (outgoing ? "" : "<") + "-[:IS_A]-" + (outgoing ? ">" : "");
        {
            List<String> cats = db.executeTransactionally("MATCH (n) WITH n ORDER BY n.id MATCH (n)" + traversePattern + "(cat:Letter) RETURN collect(cat.name) AS cats",
                    emptyMap(),
                innerResult -> Iterators.single(innerResult.columnAs("cats")));
            assertThat(cats, equalTo(asList("A", "A", "C", "B", "C")));
        }

        {

            List<String> cats = db.executeTransactionally("MATCH (n) WITH n ORDER BY n.id MATCH (n)" + traversePattern + "(cat:Letter) RETURN collect(cat.k) AS cats",
                    emptyMap(),
                    innerResult -> Iterators.single(innerResult.columnAs("cats")));
            assertThat(cats, equalTo(asList("a", "a", "c", "b", "c")));
        }

        testCall(db, "MATCH (n) WHERE n.prop IS NOT NULL RETURN count(n) AS count", (r) -> assertThat(((Number)r.get("count")).longValue(), equalTo(0L)));
    }

    @Test
    public void testCategorizeOutgoing() throws Exception {
        categorizeWithDirection(Direction.OUTGOING);
    }

    @Test
    public void testCategorizeIncoming() throws Exception {
        categorizeWithDirection(Direction.INCOMING);
    }

    @Test
    public void testCloneNodes() throws Exception {
        Node node = db.executeTransactionally("CREATE (f:Foo {name:'foo',age:42})-[:FB]->(:Bar) RETURN f", emptyMap(),
                result -> Iterators.single(result.columnAs("f")));
        TestUtil.testCall(db, "CALL apoc.refactor.cloneNodes([$node]) yield output as node return properties(node) as props,[(node)-[r]->() | type(r)] as types",
                map("node",node),
                (row) -> {
                assertEquals(map("name","foo","age",42L),row.get("props"));
                assertEquals(emptyList(),row.get("types"));
                }
        );
        TestUtil.testCall(db, "CALL apoc.refactor.cloneNodes([$node],true,[]) yield output as node return properties(node) as props,[(node)-[r]->() | type(r)] as types",
                map("node",node),
                (row) -> {
                assertEquals(map("name","foo","age",42L),row.get("props"));
                assertEquals(singletonList("FB"),row.get("types"));
                }
        );
        TestUtil.testCall(db, "CALL apoc.refactor.cloneNodes([$node],false,[]) yield output as node return properties(node) as props,[(node)-[r]->() | type(r)] as types",
                map("node",node),
                (row) -> {
                assertEquals(map("name","foo","age",42L),row.get("props"));
                assertEquals(emptyList(),row.get("types"));
                }
        );
        TestUtil.testCall(db, "CALL apoc.refactor.cloneNodes([$node],true,['age']) yield output as node return properties(node) as props,[(node)-[r]->() | type(r)] as types",
                map("node",node),
                (row) -> {
                assertEquals(map("name","foo"),row.get("props"));
                assertEquals(singletonList("FB"),row.get("types"));
                }
        );
    }

    @Test
    public void testMergeNodes() throws Exception {

    }

    @Test
    public void testChangeType() throws Exception {

    }

    @Test
    public void testRedirectRelationship() throws Exception {

    }

    @Test
    public void testMergeNodesWithConstraints() throws Exception {
        db.executeTransactionally("CREATE CONSTRAINT ON (p:Person) ASSERT p.name IS UNIQUE");
        long id = db.executeTransactionally("CREATE (p1:Person {name:'Foo'}), (p2:Person {surname:'Bar'}) RETURN id(p1) as id",
                emptyMap(),
                result -> Iterators.single(result.columnAs("id"))
        );
        testCall(db, "MATCH (o:Person {name:'Foo'}), (n:Person {surname:'Bar'}) CALL apoc.refactor.mergeNodes([o,n]) yield node return node",
                (r) -> {
                    Node node = (Node) r.get("node");
                    assertEquals(id, node.getId());
                    assertEquals(true, node.hasLabel(Label.label("Person")));
                    assertEquals("Foo", node.getProperty("name"));
                    assertEquals("Bar", node.getProperty("surname"));
                });
    }

    @Test
    public void testMergeNodesWithIngoingRelationships() throws Exception {
        long lisaId = db.executeTransactionally("CREATE \n" +
                "(alice:Person {name:'Alice'}),\n" +
                "(bob:Person {name:'Bob'}),\n" +
                "(john:Person {name:'John'}),\n" +
                "(lisa:Person {name:'Lisa'}),\n" +
                "(alice)-[:knows]->(bob),\n" +
                "(lisa)-[:knows]->(alice),\n" +
                "(bob)-[:knows]->(john) return id(lisa) as lisaId", emptyMap(),
                result -> Iterators.single(result.columnAs("lisaId")));

        //Merge (Bob) into (Lisa).
        // The updated node should have one ingoing edge from (Alice), and two outgoing edges to (John) and (Alice).
        testCall(db,
                "MATCH (bob:Person {name:'Bob'}), (lisa:Person {name:'Lisa'}) CALL apoc.refactor.mergeNodes([lisa, bob]) yield node return node, bob",
                (r)-> {
                    Node node = (Node) r.get("node");
                    assertEquals(lisaId, node.getId());
                    assertEquals("Bob", node.getProperty("name"));
                    assertEquals(1, node.getDegree(Direction.INCOMING));
                    assertEquals(2, node.getDegree(Direction.OUTGOING));
                    assertEquals("Alice", node.getRelationships(Direction.INCOMING).iterator().next().getStartNode().getProperty("name"));

                });
    }

    @Test
    public void testMergeNodesWithSelfRelationships() throws Exception {
        Map<String, Object> result = db.executeTransactionally("CREATE \n" +
                "(alice:Person {name:'Alice'}),\n" +
                "(bob:Person {name:'Bob'}),\n" +
                "(bob)-[:likes]->(bob) RETURN id(alice) AS aliceId, id(bob) AS bobId", emptyMap(),
                innerResult -> Iterators.single(innerResult));

        // Merge (bob) into (alice).
        // The updated node should have one self relationship.
        // NB: the "LIMIT 1" here is important otherwise Cypher tries to check if another MATCH is found, causing a failing read attempt to deleted node
        testCall(db,
                "MATCH (alice:Person {name:'Alice'}), (bob:Person {name:'Bob'}) WITH * LIMIT 1 CALL apoc.refactor.mergeNodes([alice, bob]) yield node return node",
                (r)-> {
                    Node node = (Node) r.get("node");
                    assertEquals(result.get("aliceId"), node.getId());
                    assertEquals("Bob", node.getProperty("name"));
                    assertEquals(1, node.getDegree(Direction.INCOMING));
                    assertEquals(1, node.getDegree(Direction.OUTGOING));
                    assertTrue(node.getSingleRelationship(RelationshipType.withName("likes"), Direction.OUTGOING).getEndNode().equals(node));
                });
    }

    @Test
    public void testMergeRelsOverwriteEagerAggregation() throws Exception {
        long id = db.executeTransactionally("Create (d:Person {name:'Daniele'})\n" + "Create (p:Country {name:'USA'})\n" + "Create (d)-[:TRAVELS_TO {year:1995, reason:\"work\"}]->(p)\n"
                + "Create (d)-[:GOES_TO {year:2010}]->(p)\n" + "Create (d)-[:FLIGHTS_TO {company:\"Air America\"}]->(p) RETURN id(p) as id ",
                emptyMap(),
                result -> Iterators.single(result.columnAs("id")));
        testCall(db, "MATCH (d:Person {name:'Daniele'})\n" + "MATCH (p:Country {name:'USA'})\n" + "MATCH (d)-[r:TRAVELS_TO]->(p)\n" + "MATCH (d)-[h:GOES_TO]->(p)\n"
                        + "MATCH (d)-[l:FLIGHTS_TO]->(p)\n" + "call apoc.refactor.mergeRelationships([r,h,l],{properties:\"overwrite\"}) yield rel\n MATCH (d)-[u]->(p) " + "return p,d,u,u.to as to, count(u) as totRel",
                (r) -> {
                    Node node = (Node) r.get("p");
                    Long totRel = (Long) r.get("totRel");
                    Relationship rel = (Relationship) r.get("u");
                    assertEquals(id, node.getId());
                    assertEquals(true, node.hasLabel(Label.label("Country")));
                    assertEquals("USA", node.getProperty("name"));
                    assertEquals(new Long(1),totRel);
                    assertEquals(true, rel.isType(RelationshipType.withName("TRAVELS_TO")));
                    assertEquals("work", rel.getProperty("reason"));
                    assertEquals(2010L, rel.getProperty("year"));
                });
    }

    @Test
    public void testMergeRelsCombineEagerAggregation() throws Exception {
        long id = db.executeTransactionally("Create (d:Person {name:'Daniele'})\n" + "Create (p:Country {name:'USA'})\n" + "Create (d)-[:TRAVELS_TO {year:1995, reason:\"work\"}]->(p)\n"
                + "Create (d)-[:GOES_TO {year:2010, reason:\"fun\"}]->(p)\n" + "Create (d)-[:FLIGHTS_TO {company:\"Air America\"}]->(p) RETURN id(p) as id ",
                emptyMap(),
                result -> Iterators.single(result.columnAs("id")));
        testCall(db, "MATCH (d:Person {name:'Daniele'})\n" + "MATCH (p:Country {name:'USA'})\n" + "MATCH (d)-[r:TRAVELS_TO]->(p)\n" + "MATCH (d)-[h:GOES_TO]->(p)\n"
                        + "MATCH (d)-[l:FLIGHTS_TO]->(p)\n" + "call apoc.refactor.mergeRelationships([r,h,l],{properties:\"discard\"}) yield rel\n MATCH (d)-[u]->(p) " + "return p,d,u,u.to as to, count(u) as totRel",
                (r) -> {
                    Node node = (Node) r.get("p");
                    Long totRel = (Long) r.get("totRel");
                    Relationship rel = (Relationship) r.get("u");
                    assertEquals(id, node.getId());
                    assertEquals(true, node.hasLabel(Label.label("Country")));
                    assertEquals("USA", node.getProperty("name"));
                    assertEquals(new Long(1),totRel);
                    assertEquals(true, rel.isType(RelationshipType.withName("TRAVELS_TO")));
                    assertEquals("work", rel.getProperty("reason"));
                    assertEquals(1995L, rel.getProperty("year"));
                });
    }

    @Test
    public void testMergeRelsEagerAggregationCombineSingleValuesProperty() throws Exception {
        long id = db.executeTransactionally("Create (d:Person {name:'Daniele'})\n" + "Create (p:Country {name:'USA'})\n" + "Create (d)-[:TRAVELS_TO {year:1995, reason:\"work\"}]->(p)\n"
                + "Create (d)-[:GOES_TO {year:2010, reason:\"fun\"}]->(p)\n" + "Create (d)-[:FLIGHTS_TO {company:\"Air America\"}]->(p) RETURN id(p) as id ",
                emptyMap(),
                result -> Iterators.single(result.columnAs("id")));
        testCall(db, "MATCH (d:Person {name:'Daniele'})\n" + "MATCH (p:Country {name:'USA'})\n" + "MATCH (d)-[r:TRAVELS_TO]->(p)\n" + "MATCH (d)-[h:GOES_TO]->(p)\n"
                        + "MATCH (d)-[l:FLIGHTS_TO]->(p)\n" + "call apoc.refactor.mergeRelationships([r,h,l],{properties:\"combine\"}) yield rel\n MATCH (d)-[u]->(p) " + "return p,d,u,u.to as to, count(u) as totRel",
                (r) -> {
                    Node node = (Node) r.get("p");
                    Long totRel = (Long) r.get("totRel");
                    Relationship rel = (Relationship) r.get("u");
                    assertEquals(id, node.getId());
                    assertEquals(true, node.hasLabel(Label.label("Country")));
                    assertEquals("USA", node.getProperty("name"));
                    assertEquals(new Long(1),totRel);
                    assertEquals(true, rel.isType(RelationshipType.withName("TRAVELS_TO")));
                    assertEquals(Arrays.asList("work", "fun").toArray(), new ArrayBackedList(rel.getProperty("reason")).toArray());
                    assertEquals(Arrays.asList(1995L, 2010L).toArray(), new ArrayBackedList(rel.getProperty("year")).toArray());
                });
    }

    @Test
    public void testMergeRelsEagerAggregationCombineArrayDifferentValuesTypeProperties() throws Exception {
        long id = db.executeTransactionally("Create (d:Person {name:'Daniele'})\n" + "Create (p:Country {name:'USA'})\n" + "Create (d)-[:TRAVELS_TO {year:1995, reason:\"work\"}]->(p)\n"
                + "Create (d)-[:GOES_TO {year:[\"2010\",\"2015\"], reason:\"fun\"}]->(p)\n" + "Create (d)-[:FLIGHTS_TO {company:\"Air America\"}]->(p) RETURN id(p) as id ",
                emptyMap(),
                result -> Iterators.single(result.columnAs("id")));
        testCall(db, "MATCH (d:Person {name:'Daniele'})\n" + "MATCH (p:Country {name:'USA'})\n" + "MATCH (d)-[r:TRAVELS_TO]->(p)\n" + "MATCH (d)-[h:GOES_TO]->(p)\n"
                        + "MATCH (d)-[l:FLIGHTS_TO]->(p)\n" + "call apoc.refactor.mergeRelationships([r,h,l],{properties:\"combine\"}) yield rel\n MATCH (d)-[u]->(p) " + "return p,d,u,u.to as to, count(u) as totRel",
                (r) -> {
                    Node node = (Node) r.get("p");
                    Long totRel = (Long) r.get("totRel");
                    Relationship rel = (Relationship) r.get("u");
                    assertEquals(id, node.getId());
                    assertEquals(true, node.hasLabel(Label.label("Country")));
                    assertEquals("USA", node.getProperty("name"));
                    assertEquals(new Long(1),totRel);
                    assertEquals(true, rel.isType(RelationshipType.withName("TRAVELS_TO")));
                    assertEquals(Arrays.asList("work", "fun").toArray(), new ArrayBackedList(rel.getProperty("reason")).toArray());
                    assertEquals(Arrays.asList("1995", "2010", "2015").toArray(), new ArrayBackedList(rel.getProperty("year")).toArray());
                });
    }

    @Test
    public void testMergeNodesAndMergeSameRelationshipWithPropertiesConfig() {
        db.executeTransactionally("create (a1:ALabel {name:'a1'})-[:HAS_REL {p:'r1'}]->(b1:BLabel {name:'b1'})," +
                "          (a2:ALabel {name:'a2'})-[:HAS_REL{p:'r2'}]->(b1)," +
                "           (a3:ALabel {name:'a3'})<-[:HAS_REL{p:'r3'}]-(b1)," +
                "           (a4:ALabel {name:'a4'})-[:HAS_REL{p:'r4'}]->(b4:BLabel {name:'b4'})");

        testCall(db, "MATCH (a1:ALabel {name:'a1'}), (a2:ALabel {name:'a2'}), (a3:ALabel {name:'a3'}), (a4:ALabel {name:'a4'}) " +
                        "     WITH head(collect([a1,a2,a3,a4])) as nodes CALL apoc.refactor.mergeNodes(nodes,{properties:'combine',mergeRels:true}) yield node return node",
                row -> {
                    assertTrue(row.get("node") != null);
                    assertTrue(row.get("node") instanceof Node);
                    Node resultingNode = (Node) row.get("node");
                    assertEquals(1, resultingNode.getDegree(Direction.INCOMING));
                    assertEquals(2,resultingNode.getDegree(Direction.OUTGOING));
                }
        );
    }

    @Test
    public void testMergeNodesAndMergeSameRelationshipsAndNodes() {
        db.executeTransactionally("Create (n1:ALabel {name:'a1'})," +
                "    (n2:ALabel {name:'a2'})," +
                "    (n3:BLabel {p1:'a3'})," +
                "     (n4:BLabel {p1:'a4'})," +
                "     (n5:CLabel {p3:'a5'})," +
                "     (n6:DLabel:Cat {p:'a6'})," +
                "     (n1)-[:HAS_REL{p:'r1'}]->(n3)," +
                "     (n2)-[:HAS_REL{p:'r2'}]->(n3)," +
                "     (n1)-[:HAS_REL{p:'r1'}]->(n4)," +
                "     (n2)-[:HAS_REL{p:'r2'}]->(n4)," +
                "     (n1)-[:HAS_REL_A{p5:'r3'}]->(n5)," +
                "     (n2)-[:HAS_REL_B{p6:'r4'}]->(n6)");

        testCall(db, "MATCH (a1:ALabel{name:'a1'}), (a2:ALabel {name:'a2'})" +
                        "     WITH [a1,a2] as nodes CALL apoc.refactor.mergeNodes(nodes,{properties:'overwrite',mergeRels:true}) yield node MATCH (n)-[r:HAS_REL]->(c:BLabel{p1:'a3'}) MATCH (n1)-[r1:HAS_REL]->(c1:BLabel{p1:'a4'}) return node, n, r ,c,n1,r1,c1 ",
                row -> {
                    assertTrue(row.get("node") != null);
                    assertTrue(row.get("node") instanceof Node);
                    Node resultingNode = (Node) row.get("node");
                    Node c = (Node) row.get("c");
                    Relationship r = (Relationship) row.get("r");
                    Relationship r1 = (Relationship)(row.get("r1"));
                    assertEquals("a2", resultingNode.getProperty("name"));
                    assertEquals(0, resultingNode.getDegree(Direction.INCOMING));
                    assertEquals(4,resultingNode.getDegree(Direction.OUTGOING));
                    assertEquals(1,c.getDegree(Direction.INCOMING));
                    assertEquals(true, r.isType(RelationshipType.withName("HAS_REL")));
                    assertEquals("r1", r.getProperty("p"));
                    assertEquals(true, r1.isType(RelationshipType.withName("HAS_REL")));
                    assertEquals("r1", r1.getProperty("p"));
                }
        );
    }

    @Test
    public void testMergeNodesAndMergeSameRelationshipsAndNodesWithoutPropertiesConfig() {
        db.executeTransactionally("Create (n1:ALabel {name:'a1'})," +
                "    (n2:ALabel {name:'a2'})," +
                "    (n3:BLabel {p1:'a3'})," +
                "     (n4:BLabel {p1:'a4'})," +
                "     (n5:CLabel {p3:'a5'})," +
                "     (n6:DLabel:Cat {p:'a6'})," +
                "     (n1)-[:HAS_REL{p:'r1'}]->(n3)," +
                "     (n2)-[:HAS_REL{p:'r2'}]->(n3)," +
                "     (n1)-[:HAS_REL{p:'r1'}]->(n4)," +
                "     (n2)-[:HAS_REL{p:'r2'}]->(n4)," +
                "     (n1)-[:HAS_REL_A{p5:'r3'}]->(n5)," +
                "     (n2)-[:HAS_REL_B{p6:'r4'}]->(n6)");

        testCall(db, "MATCH (a1:ALabel{name:'a1'}), (a2:ALabel {name:'a2'})" +
                        "     WITH [a1,a2] as nodes CALL apoc.refactor.mergeNodes(nodes,{mergeRels:true}) yield node MATCH (n)-[r:HAS_REL]->(c:BLabel{p1:'a3'}) MATCH (n1)-[r1:HAS_REL]->(c1:BLabel{p1:'a4'}) return node, n, r ,c,n1,r1,c1 ",
                row -> {
                    assertTrue(row.get("node") != null);
                    assertTrue(row.get("node") instanceof Node);
                    Node resultingNode = (Node) row.get("node");
                    Node c = (Node) row.get("c");
                    Relationship r = (Relationship) row.get("r");
                    Relationship r1 = (Relationship)(row.get("r1"));
                    assertEquals(0, resultingNode.getDegree(Direction.INCOMING));
                    assertEquals(4,resultingNode.getDegree(Direction.OUTGOING));
                    assertEquals(1,c.getDegree(Direction.INCOMING));
                    assertEquals(true, r.isType(RelationshipType.withName("HAS_REL")));
                    assertEquals(Arrays.asList( "r2" , "r1"), Arrays.asList((String[])r.getProperty("p")));
                    assertEquals(true, r1.isType(RelationshipType.withName("HAS_REL")));
                    assertEquals(Arrays.asList( "r2" , "r1"), Arrays.asList((String[])r1.getProperty("p")));
                }
        );
    }

    @Test
    public void testMergeRelsOverridePropertiesEagerAggregation() throws Exception {
        long id = db.executeTransactionally("Create (d:Person {name:'Daniele'})\n" + "Create (p:Country {name:'USA'})\n" + "Create (d)-[:TRAVELS_TO {year:1995, reason:\"work\"}]->(p)\n"
                + "Create (d)-[:GOES_TO {year:2010}]->(p)\n" + "Create (d)-[:FLIGHTS_TO {company:\"Air America\"}]->(p) RETURN id(p) as id ",
                emptyMap(),
                result -> Iterators.single(result.columnAs("id")));
        testCall(db, "MATCH (d:Person {name:'Daniele'})\n" + "MATCH (p:Country {name:'USA'})\n" + "MATCH (d)-[r:TRAVELS_TO]->(p)\n" + "MATCH (d)-[h:GOES_TO]->(p)\n"
                        + "MATCH (d)-[l:FLIGHTS_TO]->(p)\n" + "call apoc.refactor.mergeRelationships([r,h,l],{properties:\"override\"}) yield rel\n MATCH (d)-[u]->(p) " + "return p,d,u,u.to as to, count(u) as totRel",
                (r) -> {
                    Node node = (Node) r.get("p");
                    Long totRel = (Long) r.get("totRel");
                    Relationship rel = (Relationship) r.get("u");
                    assertEquals(id, node.getId());
                    assertEquals(true, node.hasLabel(Label.label("Country")));
                    assertEquals("USA", node.getProperty("name"));
                    assertEquals(new Long(1),totRel);
                    assertEquals(true, rel.isType(RelationshipType.withName("TRAVELS_TO")));
                    assertEquals("work", rel.getProperty("reason"));
                    assertEquals(2010L, rel.getProperty("year"));
                });
    }

    @Test
    public void testMergeNodesOverridePropertiesEagerAggregation() throws Exception {
        long id = db.executeTransactionally("CREATE (p1:Person {ID:1}), (p2:Person {ID:2}) RETURN id(p1) as id ",
                emptyMap(),
                result -> Iterators.single(result.columnAs("id")));
        testCall(db, "MATCH (o:Person {ID:$oldID}), (n:Person {ID:$newID}) WITH head(collect([o,n])) as nodes CALL apoc.refactor.mergeNodes(nodes, {properties:\"override\"}) yield node return node",
                map("oldID", 1L, "newID",2L),
                (r) -> {
                    Node node = (Node) r.get("node");
                    assertEquals(id, node.getId());
                    assertEquals(true, node.hasLabel(Label.label("Person")));
                    assertEquals(2L, node.getProperty("ID"));
                });
    }
}

