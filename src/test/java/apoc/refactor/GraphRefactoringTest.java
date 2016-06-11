package apoc.refactor;

import apoc.util.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.*;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testResult;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.*;

/**
 * @author mh
 * @since 25.03.16
 */
public class GraphRefactoringTest {

    private GraphDatabaseService db;

    @Before
    public void setUp() throws Exception {
        db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        TestUtil.registerProcedure(db, GraphRefactoring.class);
    }

    /*
        MATCH (o:Person {ID:{oldID}}), (n:Person {ID:{newID}}) call apoc.refactor.mergeNodes([o,n]) yield node return node
     */
    @After
    public void tearDown() {
        db.shutdown();
    }

    @Test
    public void testDeleteOneNode() throws Exception {
        long id = db.execute("CREATE (p1:Person {ID:1}), (p2:Person {ID:2}) RETURN id(p1) as id ").<Long>columnAs("id").next();
        ExecutionPlanDescription plan = db.execute("EXPLAIN MATCH (o:Person {ID:{oldID}}), (n:Person {ID:{newID}}) DELETE o RETURN o as node").getExecutionPlanDescription();
        System.out.println(plan);
        System.out.flush();
        testCall(db, "EXPLAIN MATCH (o:Person {ID:{oldID}}), (n:Person {ID:{newID}}) DELETE o RETURN o as node",
                      map("oldID", 1L, "newID",2L),
                (r) -> {
                    Node node = (Node) r.get("node");
                    assertEquals(id, node.getId());
                    assertEquals(true, node.hasLabel(Label.label("Person")));
                    assertEquals(2L, node.getProperty("ID"));
                });
    }

    @Test(expected = QueryExecutionException.class)
    public void testEagernessMergeNodesFails() throws Exception {
        db.execute("CREATE INDEX ON :Person(ID)").close();
        long id = db.execute("CREATE (p1:Person {ID:1}), (p2:Person {ID:2}) RETURN id(p1) as id ").<Long>columnAs("id").next();
        ExecutionPlanDescription plan = db.execute("EXPLAIN MATCH (o:Person {ID:{oldID}}), (n:Person {ID:{newID}}) CALL apoc.refactor.mergeNodes([o,n]) yield node return node").getExecutionPlanDescription();
        System.out.println(plan);
        System.out.flush();
        testCall(db, "MATCH (o:Person {ID:{oldID}}), (n:Person {ID:{newID}}) CALL apoc.refactor.mergeNodes([o,n]) yield node return node",
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
        long id = db.execute("CREATE (p1:Person {ID:1}), (p2:Person {ID:2}) RETURN id(p1) as id ").<Long>columnAs("id").next();
        ExecutionPlanDescription plan = db.execute("EXPLAIN MATCH (o:Person {ID:{oldID}}), (n:Person {ID:{newID}}) WITH head(collect([o,n])) as nodes CALL apoc.refactor.mergeNodes(nodes) yield node return node").getExecutionPlanDescription();
        System.out.println(plan);
        System.out.flush();
        testCall(db, "MATCH (o:Person {ID:{oldID}}), (n:Person {ID:{newID}}) WITH head(collect([o,n])) as nodes CALL apoc.refactor.mergeNodes(nodes) yield node return node",
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
        db.execute("CREATE INDEX ON :Person(ID)").close();
        long id = db.execute("CREATE (p1:Person {ID:1}), (p2:Person {ID:2}) RETURN id(p1) as id ").<Long>columnAs("id").next();
        ExecutionPlanDescription plan = db.execute("EXPLAIN MATCH (o:Person {ID:{oldID}}), (n:Person {ID:{newID}}) USING INDEX o:Person(ID) USING INDEX n:Person(ID) CALL apoc.refactor.mergeNodes([o,n]) yield node return node").getExecutionPlanDescription();
        System.out.println(plan);
        System.out.flush();
        testCall(db, "MATCH (o:Person {ID:{oldID}}), (n:Person {ID:{newID}}) USING INDEX o:Person(ID) USING INDEX n:Person(ID) CALL apoc.refactor.mergeNodes([o,n]) yield node return node",
                      map("oldID", 1L, "newID",2L),
                (r) -> {
                    Node node = (Node) r.get("node");
                    assertEquals(id, node.getId());
                    assertEquals(true, node.hasLabel(Label.label("Person")));
                    assertEquals(2L, node.getProperty("ID"));
                });
    }
    @Test
    public void testExtractNode() throws Exception {
        Long id = db.execute("CREATE (f:Foo)-[rel:FOOBAR {a:1}]->(b:Bar) RETURN id(rel) as id").<Long>columnAs("id").next();
        testCall(db, "CALL apoc.refactor.extractNode({ids},['FooBar'],'FOO','BAR')", map("ids", singletonList(id)),
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
    public void testCollapseNode() throws Exception {
        Long id = db.execute("CREATE (f:Foo)-[:FOO {a:1}]->(b:Bar {c:3})-[:BAR {b:2}]->(f) RETURN id(b) as id").<Long>columnAs("id").next();
        testCall(db, "CALL apoc.refactor.collapseNode({ids},'FOOBAR')", map("ids", singletonList(id)),
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
        db.execute("CREATE ({prop: 'Y', id:1}),({prop: 'Yes', id: 2}),({prop: 'NO', id: 3}),({prop: 'X', id: 4})").close();

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

    @Test
    public void testCategorizeOutgoing() throws Exception {
        db.execute(
                "CREATE ({prop: 'A', k: 'a', id: 1}) " +
                "CREATE ({prop: 'A', k: 'a', id: 2}) " +
                "CREATE ({prop: 'C', k: 'c', id: 3}) " +
                "CREATE ({                   id: 4}) " +
                "CREATE ({prop: 'B', k: 'b', id: 5}) " +
                "CREATE ({prop: 'C', k: 'c', id: 6})").close();

        testCall(
            db,
            "CALL apoc.refactor.categorize('prop','IS_A',true,'Letter','name',['k'],1)",
            (r) -> assertThat(((Number)r.get("count")).longValue(), equalTo(6L))
        );

        {
            Result result = db.execute("MATCH (n) WITH n ORDER BY n.id MATCH (n)-[:IS_A]->(cat:Letter) RETURN collect(cat.name) AS cats");
            List<?> cats = (List<?>) result.next().get("cats");
            result.close();

            assertThat(cats, equalTo(asList("A", "A", "C", "B", "C")));
        }

        {
            Result result = db.execute("MATCH (n) WITH n ORDER BY n.id MATCH (n)-[:IS_A]->(cat:Letter) RETURN collect(cat.k) AS cats");
            List<?> cats = (List<?>) result.next().get("cats");
            result.close();

            assertThat(cats, equalTo(asList("a", "a", "c", "b", "c")));
        }

        testCall(db, "MATCH (n) WHERE n.prop IS NOT NULL RETURN count(n) AS count", (r) -> assertThat(((Number)r.get("count")).longValue(), equalTo(0L)));
    }

    @Test
    public void testCategorizeIncoming() throws Exception {
        db.execute(
                "CREATE ({prop: 'A', k: 'a', id: 1}) " +
                "CREATE ({prop: 'A', k: 'a', id: 2}) " +
                "CREATE ({prop: 'C', k: 'c', id: 3}) " +
                "CREATE ({                   id: 4}) " +
                "CREATE ({prop: 'B', k: 'b', id: 5}) " +
                "CREATE ({prop: 'C', k: 'c', id: 6})").close();

        testCall(
                db,
                "CALL apoc.refactor.categorize('prop','IS_A',false,'Letter','name',['k'],1)",
                (r) -> assertThat(((Number)r.get("count")).longValue(), equalTo(6L))
        );

        {
            Result result = db.execute("MATCH (n) WITH n ORDER BY n.id MATCH (n)<-[:IS_A]-(cat:Letter) RETURN collect(cat.name) AS cats");
            List<?> cats = (List<?>) result.next().get("cats");
            result.close();

            assertThat(cats, equalTo(asList("A", "A", "C", "B", "C")));
        }

        {
            Result result = db.execute("MATCH (n) WITH n ORDER BY n.id MATCH (n)<-[:IS_A]-(cat:Letter) RETURN collect(cat.k) AS cats");
            List<?> cats = (List<?>) result.next().get("cats");
            result.close();

            assertThat(cats, equalTo(asList("a", "a", "c", "b", "c")));
        }

        testCall(db, "MATCH (n) WHERE n.prop IS NOT NULL RETURN count(n) AS count", (r) -> assertThat(((Number)r.get("count")).longValue(), equalTo(0L)));
    }

    @Test
    public void testCloneNodes() throws Exception {
//        TestUtil.testCall(db,
//                "",
//                (row) -> {
//
//        });
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
}
