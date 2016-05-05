package apoc.refactor;

import apoc.util.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.*;
import org.neo4j.test.TestGraphDatabaseFactory;

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

    @After
    public void tearDown() {
        db.shutdown();
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
        db.execute("CREATE ({prop: 'Y', id:1})");
        db.execute("CREATE ({prop: 'Yes', id: 2})");
        db.execute("CREATE ({prop: 'NO', id: 3})");
        db.execute("CREATE ({prop: 'X', id: 4})");

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
        db.execute("CREATE ({prop: 'A', id: 1})");
        db.execute("CREATE ({prop: 'A', id: 2})");
        db.execute("CREATE ({prop: 'C', id: 3})");
        db.execute("CREATE ({           id: 4})");
        db.execute("CREATE ({prop: 'B', id: 5})");
        db.execute("CREATE ({prop: 'C', id: 6})");

        testCall(
            db,
            "MATCH (n) WITH n ORDER BY n.id CALL apoc.refactor.categorize(n,'prop','IS_A',true,'Letter') RETURN count(n) AS count",
            (r) -> assertThat(((Number)r.get("count")).longValue(), equalTo(6L))
        );

        Result result = db.execute("MATCH (n) WITH n ORDER BY n.id MATCH (n)-[:IS_A]->(cat:Letter) RETURN collect(cat.name) AS cats");
        List<?> cats = (List<?>) result.next().get("cats");

        assertThat(cats, equalTo(asList("A", "A", "C", "B", "C")));

        testCall(db, "MATCH (n) WHERE n.prop IS NOT NULL RETURN count(n) AS count", (r) -> assertThat(((Number)r.get("count")).longValue(), equalTo(0L)));
    }

    @Test
    public void testCategorizeIncoming() throws Exception {
        db.execute("CREATE ({prop: 'A', id: 1})");
        db.execute("CREATE ({prop: 'A', id: 2})");
        db.execute("CREATE ({prop: 'C', id: 3})");
        db.execute("CREATE ({           id: 4})");
        db.execute("CREATE ({prop: 'B', id: 5})");
        db.execute("CREATE ({prop: 'C', id: 6})");

        testCall(
                db,
                "MATCH (n) WITH n ORDER BY n.id CALL apoc.refactor.categorize(n,'prop','IS_A',false,'Letter') RETURN count(n) AS count",
                (r) -> assertThat(((Number)r.get("count")).longValue(), equalTo(6L))
        );

        Result result = db.execute("MATCH (n) WITH n ORDER BY n.id MATCH (n)<-[:IS_A]-(cat:Letter) RETURN collect(cat.name) AS cats");
        List<?> cats = (List<?>) result.next().get("cats");

        assertThat(cats, equalTo(asList("A", "A", "C", "B", "C")));

        testCall(db, "MATCH (n) WHERE n.prop IS NOT NULL RETURN count(n) AS count", (r) -> assertThat(((Number)r.get("count")).longValue(), equalTo(0L)));
    }

    @Test
    public void testCategorizeWithConstraint() throws Exception {
        try(Transaction tx = db.beginTx()){
            db.schema().constraintFor(Label.label("Letter")).assertPropertyIsUnique("name");
            tx.success();
        }

        db.execute("CREATE ({prop: 'A', id: 1})");
        db.execute("CREATE ({prop: 'A', id: 2})");
        db.execute("CREATE ({prop: 'C', id: 3})");
        db.execute("CREATE ({           id: 4})");
        db.execute("CREATE ({prop: 'B', id: 5})");
        db.execute("CREATE ({prop: 'C', id: 6})");

        testCall(
                db,
                "MATCH (n) WITH n ORDER BY n.id CALL apoc.refactor.categorize(n,'prop','IS_A',true,'Letter') RETURN count(n) AS count",
                (r) -> assertThat(((Number)r.get("count")).longValue(), equalTo(6L))
        );

        Result result = db.execute("MATCH (n) WITH n ORDER BY n.id MATCH (n)-[:IS_A]->(cat:Letter) RETURN collect(cat.name) AS cats");
        List<?> cats = (List<?>) result.next().get("cats");

        assertThat(cats, equalTo(asList("A", "A", "C", "B", "C")));

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
