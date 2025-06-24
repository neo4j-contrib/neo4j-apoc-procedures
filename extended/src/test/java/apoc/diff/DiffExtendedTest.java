package apoc.diff;

import apoc.create.Create;
import apoc.util.TestUtil;
import apoc.util.collection.Iterators;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class DiffExtendedTest {

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    @BeforeClass
    public static void setup() {
        TestUtil.registerProcedure(db, DiffExtended.class, Create.class);

        try (Transaction tx = db.beginTx()) {
            Node node1Start = tx.createNode(Label.label("Node1Start"));
            Node node1End = tx.createNode(Label.label("Node1End"));
            Relationship rel1 = node1Start.createRelationshipTo(node1End, RelationshipType.withName("REL1"));
            rel1.setProperty("prop1", "val1");
            rel1.setProperty("prop2", 2L);

            Node node2Start = tx.createNode(Label.label("Node2Start"));
            Node node2End = tx.createNode(Label.label("Node2End"));
            Relationship rel2 = node2Start.createRelationshipTo(node2End, RelationshipType.withName("REL2"));
            rel2.setProperty("prop1", "val1");
            rel2.setProperty("prop2", 2L);
            rel2.setProperty("prop4", "four");

            Node node3Start = tx.createNode(Label.label("Node3Start"));
            Node node3End = tx.createNode(Label.label("Node3End"));
            Relationship rel3 = node3Start.createRelationshipTo(node3End, RelationshipType.withName("REL3"));
            rel3.setProperty("prop1", "val1");
            rel3.setProperty("prop3", "3");
            rel3.setProperty("prop4", "for");
            tx.commit();
        }
    }

    @AfterClass
    public static void teardown() {
        db.shutdown();
    }

    @Test
    public void relationshipsWithList() {
        final List<String> list = List.of("tomatoes", "bread", "cookies");
        TestUtil.testCall(
                db,
                "CREATE ()-[rel1:REL $propRel1]->(), ()-[rel2:REL $propRel2]->()\n"
                + "RETURN apoc.diff.relationships(rel1, rel2) AS result",
                Map.of(
                        "propRel1",
                        Map.of("name", "Charlie", "alpha", "one", "born", 1999, "grocery_list", list),
                        "propRel2",
                        Map.of("name", "Hannah", "beta", "two", "born", 1999, "grocery_list", list)),
                r -> {
                    Map<String, Map<String, Object>> res = (Map<String, Map<String, Object>>) r.get("result");
                    Map<String, Object> inCommon = res.get("inCommon");
                    assertArrayEquals(list.toArray(), (String[]) inCommon.get("grocery_list"));
                    assertEquals(1999, inCommon.get("born"));
                    assertEquals(Map.of("alpha", "one"), res.get("leftOnly"));
                    assertEquals(Map.of("beta", "two"), res.get("rightOnly"));
                    assertEquals(Map.of("name", Map.of("left", "Charlie", "right", "Hannah")), res.get("different"));
                });
    }

    @Test
    public void relationshipsSame() {
        String query = "MATCH (node:Node1Start)-[rel]->() RETURN apoc.diff.relationships(rel, rel) as diff";
        commonAssertionSameRels(query);
    }

    @Test
    public void relationshipsDiffering() {
        String query = "MATCH (leftNode:Node2Start)-[rel2]->(), (rightNode:Node3Start)-[rel3]->() " +
                       "RETURN apoc.diff.relationships(rel2, rel3) as diff";
        commonAssertionDifferentRels(query);
    }

    @Test
    public void shouldBeDiffWithVirtualRelationships() {
        String query = """
                MATCH (start1:Node1Start)-[rel1]->(end1), (start2:Node2Start)-[rel2]->(end2)
                WITH apoc.create.vRelationship(start1, type(rel1), {prop1: 'val1', prop2: 2, prop4: 'four'}, end1) AS relA,
                    apoc.create.vRelationship(start2, type(rel2), {prop1: 'val1', prop3: '3', prop4: 'for'}, end2) AS relB
                RETURN apoc.diff.relationships(relA, relB) as diff""";
        commonAssertionDifferentRels(query);
    }

    @Test
    public void shouldBeSameWithVirtualRelationships() {
        String query = "MATCH (start:Node1Start)-[rel]->(end)" +
                       "WITH apoc.create.vRelationship(start, type(rel), {prop1: 'val1', prop2: 2}, end) AS rel "
                       + "RETURN apoc.diff.relationships(rel, rel) as diff";
        commonAssertionSameRels(query);
    }

    private void commonAssertionDifferentRels(String query) {
        Map<String, Object> result = db.executeTransactionally(
                query,
                Map.of(),
                r -> Iterators.single(r.columnAs("diff")));
        assertNotNull(result);

        HashMap<String, Object> leftOnly = (HashMap<String, Object>) result.get("leftOnly");
        assertEquals(1, leftOnly.size());
        assertEquals(2L, leftOnly.get("prop2"));

        HashMap<String, Object> rightOnly = (HashMap<String, Object>) result.get("rightOnly");
        assertEquals(1, rightOnly.size());
        assertEquals("3", rightOnly.get("prop3"));

        HashMap<String, HashMap<String, Object>> different =
                (HashMap<String, HashMap<String, Object>>) result.get("different");
        assertEquals(1, different.size());
        HashMap<String, Object> pairs = different.get("prop4");
        assertEquals("four", pairs.get("left"));
        assertEquals("for", pairs.get("right"));

        HashMap<String, Object> inCommon = (HashMap<String, Object>) result.get("inCommon");
        assertEquals(1, inCommon.size());
        assertEquals("val1", inCommon.get("prop1"));
    }

    private void commonAssertionSameRels(String query) {
        Map<String, Object> result =
                db.executeTransactionally(query, Map.of(), r -> Iterators.single(r.columnAs("diff")));
        assertNotNull(result);

        HashMap<String, Object> leftOnly = (HashMap<String, Object>) result.get("leftOnly");
        assertTrue(leftOnly.isEmpty());

        HashMap<String, Object> rightOnly = (HashMap<String, Object>) result.get("rightOnly");
        assertTrue(rightOnly.isEmpty());

        HashMap<String, Object> different = (HashMap<String, Object>) result.get("different");
        assertTrue(different.isEmpty());

        HashMap<String, Object> inCommon = (HashMap<String, Object>) result.get("inCommon");
        assertEquals(2, inCommon.size());
        assertEquals("val1", inCommon.get("prop1"));
        assertEquals(2L, inCommon.get("prop2"));
    }
}
