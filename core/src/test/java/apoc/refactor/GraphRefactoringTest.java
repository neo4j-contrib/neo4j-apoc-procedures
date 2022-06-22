package apoc.refactor;

import apoc.util.ArrayBackedList;
import apoc.util.TestUtil;
import apoc.util.Util;
import org.apache.commons.collections4.IteratorUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testCallCount;
import static apoc.util.TestUtil.testCallEmpty;
import static apoc.util.TestUtil.testResult;
import static apoc.util.Util.isSelfRel;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.neo4j.configuration.SettingImpl.newBuilder;
import static org.neo4j.configuration.SettingValueParsers.BOOL;
import static org.neo4j.graphdb.Label.label;

/**
 * @author mh
 * @since 25.03.16
 */
public class GraphRefactoringTest {
    protected static final String CLONE_NODES_QUERY = "match (n:MyBook) with n call apoc.refactor.cloneNodes([n], true) " +
            "YIELD output, error RETURN output, error";
    protected static final String CLONE_SUBGRAPH_QUERY = "MATCH (n:MyBook) with n call apoc.refactor.cloneSubgraph([n], [], {}) YIELD output, error RETURN output, error";
    protected static final String EXTRACT_QUERY = "MATCH p=(:Start)-[r:TO_MOVE]->(:End) with r call apoc.refactor.extractNode([r], ['MyBook'], 'OUT', 'IN') " +
            "YIELD output, error RETURN output, error";

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule()
            .withSetting(newBuilder( "unsupported.dbms.debug.track_cursor_close", BOOL, false ).build(), false)
            .withSetting(newBuilder( "unsupported.dbms.debug.trace_cursors", BOOL, false ).build(), false);

    @Before
    public void setUp() throws Exception {
        TestUtil.registerProcedure(db, GraphRefactoring.class);
    }

    @After
    public void tearDown() {
        db.shutdown();
    }

    @Test
    public void deleteAndReconnect() throws Exception {
        db.executeTransactionally("CREATE (f:One)-[:ALPHA {a:'b'}]->(b:Two)-[:BETA {c:'d', e:'f'}]->(c:Three)-[:GAMMA]->(d:Four)-[:DELTA {aa: 'bb', cc: 'dd', ee: 'ff'}]->(e:Five {foo: 'bar', baz: 'baa'})");

        TestUtil.testCall(db, Util.NODE_COUNT, (row) -> assertEquals(5L, row.get("result")));

        TestUtil.testCallEmpty(db, "MATCH p=(f:One)-->(b:Two)-->(c:Three), (d:Four), (e:Five) WITH p, [d,e] as list CALL apoc.refactor.deleteAndReconnect(p, list) YIELD nodes, relationships RETURN nodes, relationships", emptyMap());

        TestUtil.testCall(db, Util.NODE_COUNT, (row) -> assertEquals(5L, row.get("result")));

        TestUtil.testCall(db, "MATCH p=(f:One)-->(b:Two)-->(c:Three)-->(d:Four)-->(e:Five) WITH p, [b,d] as list CALL apoc.refactor.deleteAndReconnect(p, list) YIELD nodes, relationships RETURN nodes, relationships",
                (row) -> {
                    List<Node> nodes = (List<Node>) row.get("nodes");
                    assertEquals(3, nodes.size());
                    Node node1 = nodes.get(0);
                    assertEquals(singletonList(label("One")), node1.getLabels());
                    Node node2 = nodes.get(1);
                    assertEquals(singletonList(label("Three")), node2.getLabels());
                    Node node3 = nodes.get(2);
                    assertEquals(singletonList(label("Five")), node3.getLabels());
                    List<Relationship> rels = (List<Relationship>) row.get("relationships");
                    assertEquals(2, rels.size());
                    Relationship rel1 = rels.get(0);
                    assertEquals("ALPHA", rel1.getType().name());
                    assertEquals("b", rel1.getProperty("a"));
                    Relationship rel2 = rels.get(1);
                    assertEquals("GAMMA", rel2.getType().name());
                    assertTrue(rel2.getAllProperties().isEmpty());
                    assertNotNull(row.get("nodes"));
                });

        TestUtil.testCall(db, Util.NODE_COUNT, (row) -> assertEquals(3L, row.get("result")));
    }

    @Test
    public void deleteAndReconnectWithTerminalNodes() throws Exception {
        db.executeTransactionally("CREATE (f:One)-[:ALPHA {a:'b'}]->(c:Two)-[:GAMMA]->(e:Three {foo: 'bar', baz: 'baa'})");

        // - terminal node
        TestUtil.testCall(db, "MATCH p=(f:One)-->(c:Two)-->(e:Three) WITH p, f CALL apoc.refactor.deleteAndReconnect(p, [f]) YIELD nodes, relationships RETURN nodes, relationships",
                (row) -> {
                    List<Node> nodes = (List<Node>) row.get("nodes");
                    assertEquals(2, nodes.size());
                    Node node1 = nodes.get(0);
                    assertEquals(singletonList(label("Two")), node1.getLabels());
                    Node node2 = nodes.get(1);
                    assertEquals(singletonList(label("Three")), node2.getLabels());
                    assertEquals("bar", node2.getProperty("foo"));
                    assertEquals("baa", node2.getProperty("baz"));
                    List<Relationship> rels = (List<Relationship>) row.get("relationships");
                    assertEquals(1, rels.size());
                    Relationship rel = rels.get(0);
                    assertEquals("GAMMA", rel.getType().name());
                    assertTrue(rel.getAllProperties().isEmpty());
                    assertNotNull(row.get("nodes"));
                });

        TestUtil.testCall(db, Util.NODE_COUNT, (row) -> assertEquals(2L, row.get("result")));

        TestUtil.testCall(db, "MATCH p=(f:Three) WITH p, [f] as list CALL apoc.refactor.deleteAndReconnect(p, list) YIELD nodes, relationships RETURN nodes, relationships",
                (row) -> {
                    assertEquals(0, ((List<Node>) row.get("nodes")).size());
                    assertEquals(0, ((List<Node>) row.get("relationships")).size());
                });

        TestUtil.testCall(db, Util.NODE_COUNT, (row) -> assertEquals(1L, row.get("result")));
    }

    @Test
    public void deleteAndReconnectConsecutiveNodes() throws Exception {
        db.executeTransactionally("CREATE (f:Alpha)-[:REL_1 {a:'b'}]->(b:Beta)-[:REL_2 {c:'d', e:'f'}]->(c:Gamma)-[:REL_3]->(d:Delta)-[:REL_4 {aa: 'bb', cc: 'dd', ee: 'ff'}]->(e:Epsilon {foo: 'bar', baz: 'baa'})");

        TestUtil.testCall(db, Util.NODE_COUNT, (row) -> assertEquals(5L, row.get("result")));

        TestUtil.testCall(db, "MATCH p=(f:Alpha)-->(b:Beta)-->(c:Gamma)-->(d:Delta)-->(e:Epsilon) WITH p, [b,c] as list CALL apoc.refactor.deleteAndReconnect(p, list) YIELD nodes, relationships RETURN nodes, relationships",
                (row) -> {
                    List<Node> nodes = (List<Node>) row.get("nodes");
                    assertEquals(3, nodes.size());
                    Node node1 = nodes.get(0);
                    assertEquals(singletonList(label("Alpha")), node1.getLabels());
                    Node node2 = nodes.get(1);
                    assertEquals(singletonList(label("Delta")), node2.getLabels());
                    Node node3 = nodes.get(2);
                    assertEquals(singletonList(label("Epsilon")), node3.getLabels());
                    List<Relationship> rels = (List<Relationship>) row.get("relationships");
                    assertEquals(2, rels.size());
                    Relationship rel1 = rels.get(0);
                    assertEquals("REL_4", rel1.getType().name());
                    assertEquals("bb", rel1.getProperty("aa"));
                    assertEquals("dd", rel1.getProperty("cc"));
                    assertEquals("ff", rel1.getProperty("ee"));
                    Relationship rel2 = rels.get(1);
                    assertEquals("REL_1", rel2.getType().name());
                    assertEquals("b", rel2.getProperty("a"));
                    assertNotNull(row.get("nodes"));
                });

        TestUtil.testCall(db, Util.NODE_COUNT, (row) -> assertEquals(3L, row.get("result")));

    }

    @Test
    public void deleteAndReconnectWithIncomingRelConfig() throws Exception {
        db.executeTransactionally("CREATE (f:One)-[:ALPHA {a:'b'}]->(b:Two)-[:BETA {c:'d', e:'f'}]->(c:Three)-[:GAMMA]->(d:Four)-[:DELTA {aa: 'bb', cc: 'dd', ee: 'ff'}]->(e:Five {foo: 'bar', baz: 'baa'})");

        TestUtil.testCall(db, Util.NODE_COUNT, (row) -> assertEquals(5L, row.get("result")));

        TestUtil.testCall(db, "MATCH p=(f:One)-->(b:Two)-->(c:Three)-->(d:Four)-->(e:Five) WITH p, [b,d] as list CALL apoc.refactor.deleteAndReconnect(p, list, {relationshipSelectionStrategy: 'incoming'}) YIELD nodes, relationships RETURN nodes, relationships",
                (row) -> {
                    List<Node> nodes = (List<Node>) row.get("nodes");
                    assertEquals(3, nodes.size());
                    Node node1 = nodes.get(0);
                    assertEquals(singletonList(label("One")), node1.getLabels());
                    Node node2 = nodes.get(1);
                    assertEquals(singletonList(label("Three")), node2.getLabels());
                    Node node3 = nodes.get(2);
                    assertEquals(singletonList(label("Five")), node3.getLabels());
                    List<Relationship> rels = (List<Relationship>) row.get("relationships");
                    assertEquals(2, rels.size());
                    Relationship rel1 = rels.get(0);
                    assertEquals("ALPHA", rel1.getType().name());
                    assertEquals("b", rel1.getProperty("a"));
                    Relationship rel2 = rels.get(1);
                    assertEquals("GAMMA", rel2.getType().name());
                    assertNotNull(row.get("nodes"));
                });

        TestUtil.testCall(db, Util.NODE_COUNT, (row) -> assertEquals(3L, row.get("result")));
    }

    @Test
    public void deleteAndReconnectWithOutgoingRelConfig() throws Exception {
        db.executeTransactionally("CREATE (f:One)-[:ALPHA {a:'b'}]->(b:Two)-[:BETA {c:'d', e:'f'}]->(c:Three)-[:GAMMA]->(d:Four)-[:DELTA {aa: 'bb', cc: 'dd', ee: 'ff'}]->(e:Five {foo: 'bar', baz: 'baa'})");

        TestUtil.testCall(db, Util.NODE_COUNT, (row) -> assertEquals(5L, row.get("result")));

        TestUtil.testCall(db, "MATCH p=(f:One)-->(b:Two)-->(c:Three)-->(d:Four)-->(e:Five) WITH p, [b,d] as list CALL apoc.refactor.deleteAndReconnect(p, list, {relationshipSelectionStrategy: 'outgoing'}) YIELD nodes, relationships RETURN nodes, relationships",
                (row) -> {
                    List<Node> nodes = (List<Node>) row.get("nodes");
                    assertEquals(3, nodes.size());
                    Node node1 = nodes.get(0);
                    assertEquals(singletonList(label("One")), node1.getLabels());
                    Node node2 = nodes.get(1);
                    assertEquals(singletonList(label("Three")), node2.getLabels());
                    Node node3 = nodes.get(2);
                    assertEquals(singletonList(label("Five")), node3.getLabels());
                    List<Relationship> rels = (List<Relationship>) row.get("relationships");
                    assertEquals(2, rels.size());
                    Relationship rel1 = rels.get(0);
                    assertEquals("BETA", rel1.getType().name());
                    assertEquals("d", rel1.getProperty("c"));
                    assertEquals("f", rel1.getProperty("e"));
                    Relationship rel2 = rels.get(1);
                    assertEquals("DELTA", rel2.getType().name());
                    assertEquals("bb", rel2.getProperty("aa"));
                    assertEquals("dd", rel2.getProperty("cc"));
                    assertEquals("ff", rel2.getProperty("ee"));
                    assertNotNull(row.get("nodes"));
                });

        TestUtil.testCall(db, Util.NODE_COUNT, (row) -> assertEquals(3L, row.get("result")));
    }

    @Test
    public void deleteAndReconnectWithMergeRelConfig() throws Exception {
        db.executeTransactionally("CREATE (f:One)-[:ALPHA {a:'b'}]->(b:Two)-[:BETA {a:'d', e:'f', g: 'h'}]->(c:Three)-[:GAMMA {aa: 'one'}]->(d:Four)-[:DELTA {aa: 'bb', cc: 'dd', ee: 'ff'}]->(e:Five {foo: 'bar', baz: 'baa'})");

        TestUtil.testCall(db, Util.NODE_COUNT, (row) -> assertEquals(5L, row.get("result")));

        TestUtil.testCall(db, "MATCH p=(f:One)-->(b:Two)-->(c:Three)-->(d:Four)-->(e:Five) WITH p, [b,d] as list CALL apoc.refactor.deleteAndReconnect(p, list, {properties: 'discard', relationshipSelectionStrategy: 'merge'}) YIELD nodes, relationships RETURN nodes, relationships",
                (row) -> {
                    List<Node> nodes = (List<Node>) row.get("nodes");
                    assertEquals(3, nodes.size());
                    Node node1 = nodes.get(0);
                    assertEquals(singletonList(label("One")), node1.getLabels());
                    Node node2 = nodes.get(1);
                    assertEquals(singletonList(label("Three")), node2.getLabels());
                    Node node3 = nodes.get(2);
                    assertEquals(singletonList(label("Five")), node3.getLabels());
                    List<Relationship> rels = (List<Relationship>) row.get("relationships");
                    assertEquals(2, rels.size());
                    Relationship rel1 = rels.get(0);
                    assertEquals("ALPHA_BETA", rel1.getType().name());
                    assertEquals("f", rel1.getProperty("e"));
                    assertEquals("b", rel1.getProperty("a"));
                    assertEquals("h", rel1.getProperty("g"));
                    Relationship rel2 = rels.get(1);
                    assertEquals("GAMMA_DELTA", rel2.getType().name());
                    assertEquals("one", rel2.getProperty("aa"));
                    assertEquals("dd", rel2.getProperty("cc"));
                    assertEquals("ff", rel2.getProperty("ee"));
                    assertNotNull(row.get("nodes"));
                });

        TestUtil.testCall(db, Util.NODE_COUNT, (row) -> assertEquals(3L, row.get("result")));
    }

    @Test
    public void deleteAndReconnectWithMergeRelConfigAndPropertiesCombine() throws Exception {
        db.executeTransactionally("CREATE (f:One)-[:ALPHA {a:'b'}]->(b:Two)-[:BETA {a:'d', e:'f', g: 'h'}]->(c:Three)-[:GAMMA {aa: 'one'}]->(d:Four)-[:DELTA {aa: 'bb', cc: 'dd', ee: 'ff'}]->(e:Five {foo: 'bar', baz: 'baa'})");

        TestUtil.testCall(db, Util.NODE_COUNT, (row) -> assertEquals(5L, row.get("result")));

        TestUtil.testCall(db, "MATCH p=(f:One)-->(b:Two)-->(c:Three)-->(d:Four)-->(e:Five) WITH p, [b,d] as list CALL apoc.refactor.deleteAndReconnect(p, list, {properties: 'combine', relationshipSelectionStrategy: 'merge'}) YIELD nodes, relationships RETURN nodes, relationships",
                (row) -> {
                    List<Node> nodes = (List<Node>) row.get("nodes");
                    assertEquals(3, nodes.size());
                    Node node1 = nodes.get(0);
                    assertEquals(singletonList(label("One")), node1.getLabels());
                    Node node2 = nodes.get(1);
                    assertEquals(singletonList(label("Three")), node2.getLabels());
                    Node node3 = nodes.get(2);
                    assertEquals(singletonList(label("Five")), node3.getLabels());
                    List<Relationship> rels = (List<Relationship>) row.get("relationships");
                    assertEquals(2, rels.size());
                    Relationship rel1 = rels.get(0);
                    assertEquals("ALPHA_BETA", rel1.getType().name());
                    assertEquals("f", rel1.getProperty("e"));
                    assertThat(Arrays.asList((String[])rel1.getProperty("a")), containsInAnyOrder("b", "d"));
                    assertEquals("h", rel1.getProperty("g"));
                    Relationship rel2 = rels.get(1);
                    assertEquals("GAMMA_DELTA", rel2.getType().name());
                    assertThat(Arrays.asList((String[])rel2.getProperty("aa")), containsInAnyOrder("one", "bb"));
                    assertEquals("ff", rel2.getProperty("ee"));
                    assertEquals("dd", rel2.getProperty("cc"));
                    assertNotNull(row.get("nodes"));
                });

        TestUtil.testCall(db, Util.NODE_COUNT, (row) -> assertEquals(3L, row.get("result")));
    }

    @Test
    public void deleteAndReconnectWithMergeRelConfigAndPropertiesOverride() throws Exception {
        db.executeTransactionally("CREATE (f:One)-[:ALPHA {a:'b'}]->(b:Two)-[:BETA {a:'d', e:'f', g: 'h'}]->(c:Three)-[:GAMMA {aa: 'one'}]->(d:Four)-[:DELTA {aa: 'bb', cc: 'dd', ee: 'ff'}]->(e:Five {foo: 'bar', baz: 'baa'})");

        TestUtil.testCall(db, Util.NODE_COUNT, (row) -> assertEquals(5L, row.get("result")));

        TestUtil.testCall(db, "MATCH p=(f:One)-->(b:Two)-->(c:Three)-->(d:Four)-->(e:Five) WITH p, [b,d] as list CALL apoc.refactor.deleteAndReconnect(p, list, {properties: 'override', relationshipSelectionStrategy: 'merge'}) YIELD nodes, relationships RETURN nodes, relationships",
                (row) -> {
                    List<Node> nodes = (List<Node>) row.get("nodes");
                    assertEquals(3, nodes.size());
                    Node node1 = nodes.get(0);
                    assertEquals(singletonList(label("One")), node1.getLabels());
                    Node node2 = nodes.get(1);
                    assertEquals(singletonList(label("Three")), node2.getLabels());
                    Node node3 = nodes.get(2);
                    assertEquals(singletonList(label("Five")), node3.getLabels());
                    List<Relationship> rels = (List<Relationship>) row.get("relationships");
                    assertEquals(2, rels.size());
                    Relationship rel1 = rels.get(0);
                    assertEquals("ALPHA_BETA", rel1.getType().name());
                    assertEquals("f", rel1.getProperty("e"));
                    assertEquals("d", rel1.getProperty("a"));
                    assertEquals("h", rel1.getProperty("g"));
                    Relationship rel2 = rels.get(1);
                    assertEquals("GAMMA_DELTA", rel2.getType().name());
                    assertEquals("bb", rel2.getProperty("aa"));
                    assertEquals("dd", rel2.getProperty("cc"));
                    assertEquals("ff", rel2.getProperty("ee"));
                    assertNotNull(row.get("nodes"));
                });

        TestUtil.testCall(db, Util.NODE_COUNT, (row) -> assertEquals(3L, row.get("result")));
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
    public void testRefactorWithSameEntities() {
        Node node = db.executeTransactionally("CREATE (n:SingleNode) RETURN n", emptyMap(), 
                r -> Iterators.single(r.columnAs("n")));
        testCall(db, "MATCH (n:SingleNode) CALL apoc.refactor.mergeNodes([n,n]) yield node return node",
                r -> assertEquals(node, r.get("node")));
        testCallCount(db, "MATCH (n:SingleNode) RETURN n", 1);

        Relationship rel = db.executeTransactionally("CREATE (n:Start)-[r:REL_TO_MERGE]->(:End) RETURN r", emptyMap(), 
                r -> Iterators.single(r.columnAs("r")));
        testCall(db, "MATCH (n:Start)-[r:REL_TO_MERGE]->(:End) CALL apoc.refactor.mergeRelationships([r,r]) yield rel return rel", r -> {
            assertEquals(rel, r.get("rel"));
        });
        testCallCount(db, "MATCH (n:Start)-[r:REL_TO_MERGE]->(:End) RETURN r", 1);
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
        final String label = "Letter";
        final String targetKey = "name";
        db.executeTransactionally("CREATE CONSTRAINT ON (n:`" + label + "`) ASSERT n.`" + targetKey + "` IS UNIQUE");

        testCallEmpty(
                db,
                "CALL apoc.refactor.categorize('prop', 'IS_A', $direction, $label, $targetKey, ['k'], 1)",
                map("direction", outgoing, "label", label, "targetKey", targetKey)
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
        db.executeTransactionally("DROP CONSTRAINT ON (n:`" + label + "`) ASSERT n.`" + targetKey + "` IS UNIQUE");
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
    public void testIssue3000() {
        db.executeTransactionally("CREATE (a:Person {name: 'Mark', city: 'London'})\n" +
                "CREATE (b:Person {name: 'Dan', city: 'Hull'})\n" +
                "CREATE (a)-[r:FRIENDS_WITH]->(b)");
        
        testResult(db, "MATCH (p:Person) WITH collect(p) as people \n" +
                        "CALL apoc.refactor.cloneNodes(people, true) \n" +
                        "YIELD output \n" +
                        "RETURN output ORDER BY output.name",
                (row) -> {
                    final ResourceIterator<Node> nodes = row.columnAs("output");
                    final Node first = nodes.next();
                    assertEquals("Dan", first.getProperty("name"));
                    first.getRelationships()
                            .forEach(i -> assertEquals("Mark", i.getStartNode().getProperty("name")));
                    final Node second = nodes.next();
                    assertEquals("Mark", second.getProperty("name"));
                    second.getRelationships()
                            .forEach(i -> assertEquals("Dan", i.getEndNode().getProperty("name")));
                    assertFalse(nodes.hasNext());
                }
        );
    }

    @Test
    public void testCloneNodes() throws Exception {
        Long nodeId = db.executeTransactionally("CREATE (f:Foo {name:'foo',age:42})-[:FB]->(:Bar) RETURN id(f) AS nodeId", emptyMap(),
                result -> Iterators.single(result.columnAs("nodeId")));
        TestUtil.testCall(db, "MATCH (n:Foo) WHERE id(n) = $nodeId CALL apoc.refactor.cloneNodes([n]) yield output as node return properties(node) as props,[(node)-[r]->() | type(r)] as types",
                map("nodeId", nodeId),
                (row) -> {
                assertEquals(map("name","foo","age",42L),row.get("props"));
                assertEquals(emptyList(),row.get("types"));
                }
        );
        TestUtil.testCall(db, "MATCH (n:Foo) WHERE id(n) = $nodeId CALL apoc.refactor.cloneNodes([n],true,[]) yield output as node return properties(node) as props,[(node)-[r]->() | type(r)] as types",
                map("nodeId", nodeId),
                (row) -> {
                assertEquals(map("name","foo","age",42L),row.get("props"));
                assertEquals(singletonList("FB"),row.get("types"));
                }
        );
        TestUtil.testCall(db, "MATCH (n:Foo) WHERE id(n) = $nodeId CALL apoc.refactor.cloneNodes([n],false,[]) yield output as node return properties(node) as props,[(node)-[r]->() | type(r)] as types",
                map("nodeId", nodeId),
                (row) -> {
                assertEquals(map("name","foo","age",42L),row.get("props"));
                assertEquals(emptyList(),row.get("types"));
                }
        );
        TestUtil.testCall(db, "MATCH (n:Foo) WHERE id(n) = $nodeId CALL apoc.refactor.cloneNodes([n],true,['age']) yield output as node return properties(node) as props,[(node)-[r]->() | type(r)] as types",
                map("nodeId", nodeId),
                (row) -> {
                assertEquals(map("name","foo"),row.get("props"));
                assertEquals(singletonList("FB"),row.get("types"));
                }
        );
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

    @Test
    public void testMergeNodesOnArrayValues() throws Exception {
        long id = db.executeTransactionally("CREATE (p1:Person {ID:1, prop: ['foo']}), (p2:Person {ID:2, prop: ['foo']}) RETURN id(p1) as id ",
                emptyMap(),
                result -> Iterators.single(result.columnAs("id")));
        testCall(db, "MATCH (o:Person {ID:$oldID}), (n:Person {ID:$newID}) WITH head(collect([o,n])) as nodes CALL apoc.refactor.mergeNodes(nodes, {properties:'combine'}) yield node return node",
                map("oldID", 1L, "newID",2L),
                (r) -> {
                    Node node = (Node) r.get("node");
                    assertEquals(id, node.getId());
                    assertEquals(true, node.hasLabel(Label.label("Person")));
                    assertArrayEquals(new long[] {1L, 2L}, (long[]) node.getProperty("ID"));
                    assertEquals("foo", node.getProperty("prop"));
                });
    }

    @Test
    public void testMergeNodesOnArrayValuesPreventTypeChange() throws Exception {
        long id = db.executeTransactionally("CREATE (p1:Person {ID:1, prop: ['foo']}), (p2:Person {ID:2, prop: ['foo']}) RETURN id(p1) as id ",
                emptyMap(),
                result -> Iterators.single(result.columnAs("id")));
        testCall(db, "MATCH (o:Person {ID:$oldID}), (n:Person {ID:$newID}) WITH head(collect([o,n])) as nodes CALL apoc.refactor.mergeNodes(nodes, {properties:'combine', singleElementAsArray: true}) yield node return node",
                map("oldID", 1L, "newID",2L),
                (r) -> {
                    Node node = (Node) r.get("node");
                    assertEquals(id, node.getId());
                    assertEquals(true, node.hasLabel(Label.label("Person")));
                    assertArrayEquals(new long[] {1L, 2L}, (long[]) node.getProperty("ID"));
                    assertArrayEquals(new String[] {"foo"}, (String[]) node.getProperty("prop"));
                });
    }

    @Test(expected = QueryExecutionException.class)
    public void testRefactorCategorizeExceptionWithNoConstraint() {
        // given
        final String label = "Country";
        final String targetKey = "name";
        db.executeTransactionally("with [\"IT\", \"DE\"] as countries\n" +
                "unwind countries as country\n" +
                "foreach (no in RANGE(1, 4) |\n" +
                "  create (n:Company {name: country + no, country: country})\n" +
                ")");

        // when
        try {
            db.executeTransactionally("CALL apoc.refactor.categorize('country', 'OPERATES_IN', true, $label, $targetKey, [], 1)",
                    map("label", label, "targetKey", targetKey));
        } catch (QueryExecutionException e) {
            // then
            String expectedMessage = "Before execute this procedure you must define an unique constraint for the label and the targetKey:\n" +
                    "CREATE CONSTRAINT ON (n:`" + label + "`) ASSERT n.`" + targetKey + "` IS UNIQUE";
            assertEquals(expectedMessage, ExceptionUtils.getRootCause(e).getMessage());
            throw e;
        }
    }

    @Test
    public void testRefactorCategorizeNoDups() {
        // given
        final String label = "Country";
        final String targetKey = "name";
        db.executeTransactionally("CREATE CONSTRAINT ON (n:`" + label + "`) ASSERT n.`" + targetKey + "` IS UNIQUE");
        db.executeTransactionally("with [\"IT\", \"DE\"] as countries\n" +
                "unwind countries as country\n" +
                "foreach (no in RANGE(1, 4) |\n" +
                "  create (n:Company {name: country + no, country: country})\n" +
                ")");

        // when
        db.executeTransactionally("CALL apoc.refactor.categorize('country', 'OPERATES_IN', true, $label, $targetKey, [], 1)",
                map("label", label, "targetKey", targetKey));

        // then
        final long countries = TestUtil.singleResultFirstColumn(db, "MATCH (c:Country) RETURN count(c) AS countries");
        assertEquals(2, countries);
        final List<String> countryNames = TestUtil.firstColumn(db, "MATCH (c:Country) RETURN c.name");
        assertThat(countryNames, Matchers.containsInAnyOrder("IT", "DE"));

        final long relsCount = TestUtil.singleResultFirstColumn(db, "MATCH p = (c:Company)-[:OPERATES_IN]->(cc:Country) RETURN count(p) AS relsCount");
        assertEquals(8, relsCount);
        db.executeTransactionally("DROP CONSTRAINT ON (n:`" + label + "`) ASSERT n.`" + targetKey + "` IS UNIQUE");
    }

    @Test
    public void testMergeNodeShouldNotCreateSelfRelationshipsInPreExistingSelfRel() {
        db.executeTransactionally("CREATE (a:TestNode {a:'a'})-[:TEST_REL]->(b:TestNode {a:'b'})-[:TEST_REL]->(c:TestNode {a:'c'})\n" +
                "WITH a, c CREATE (a)-[:TEST_REL {prop: 'one'}]->(a), (a)-[:TEST_REL {prop: 'two'}]->(a) WITH c CREATE (c)-[:TEST_REL]->(c);");
        testCall(db, "MATCH (n:TestNode) WITH collect(n) as nodes CALL apoc.refactor.mergeNodes(nodes, {mergeRels: true, produceSelfRel: false}) yield node return node",
                (r) -> {
                    Node node = (Node) r.get("node");
                    Iterator<Relationship> relIterator = node.getRelationships().iterator();
                    final String expectedRelType = "TEST_REL";
                    final Relationship firstRel = relIterator.next();
                    assertSelfRel(firstRel, expectedRelType);
                    assertEquals(Map.of("prop", "two"), firstRel.getAllProperties());
                    final Relationship secondRel = relIterator.next();
                    assertSelfRel(secondRel, expectedRelType);
                    assertEquals(Map.of("prop", "one"), secondRel.getAllProperties());
                    assertFalse(relIterator.hasNext());
                });
    }

    @Test
    public void shouldAlwaysOverrideNodePropsIfNotSetAndCombineRelPropsIfPropertyIsNull() {
        // test case from https://trello.com/c/7yO7mniS/924-s2cast-softwareapocrefactormergenodes-is-not-producing-desired-output
        final String query = "CREATE (n1:Test:Obj {Name:1})\n" +
                "CREATE (n2:Test:Obj {Name:2})\n" +
                "CREATE (t:Test:Tran {Name:'t'})\n" +
                "MERGE (t)-[:Contains {isReduced:true}]->(n1)\n" +
                "MERGE (t)-[:Contains {isReduced:false, onlyForn2:true}]->(n2)\n" +
                "WITH collect(n1) + collect(n2) AS nodes\n" +
                "CALL apoc.refactor.mergeNodes(nodes, $config)\n" +
                "YIELD node WITH node\n" +
                "MATCH (node)-[r]-(t:Test:Tran) RETURN node, collect(r) AS rels";
        
        testCall(db, query, map("config", map()), r -> {
            assertOverrideNode(r);
            final List<Relationship> rels = (List<Relationship>) r.get("rels");
            assertEquals(2, rels.size());
        });
        
        testCall(db, query, map("config", 
                map("mergeRels", true, "produceSelfRel", false, "properties", null)
        ), r -> {
            assertOverrideNode(r);
            final List<Relationship> rels = (List<Relationship>) r.get("rels");
            assertEquals(1, rels.size());
            assertArrayEquals(new boolean[] {true, false}, (boolean[]) rels.get(0).getProperty("isReduced"));
        });

        testCall(db, query, map("config", 
                map("properties", map())
        ), r -> {
            assertOverrideNode(r);
            final List<Relationship> rels = (List<Relationship>) r.get("rels");
            assertEquals(2, rels.size());
        });

        testCall(db, query, map("config", 
                map("mergeRels", true, "properties", map())
        ), r -> {
            assertOverrideNode(r);
            final List<Relationship> rels = (List<Relationship>) r.get("rels");
            assertEquals(1, rels.size());
            assertEquals(false, rels.get(0).getProperty("isReduced"));
        });
    }

    private void assertOverrideNode(Map<String, Object> r) {
        final Node node = (Node) r.get("node");
        assertEquals(2L, node.getProperty("Name"));
    }

    @Test
    public void testMergeNodeShouldNotCreateSelfRelationshipsAndCancelThePreExistingSelfRelAfterMerge() {
        db.executeTransactionally("CREATE (a:TestNode {a:'a'})-[:TEST_REL]->(b:TestNode {a:'b'})-[:TEST_REL]->(c:TestNode {a:'c'})\n" +
                "WITH a, c CREATE (a)-[:TEST_REL {prop: 'one'}]->(a), (a)-[:TEST_REL {prop: 'two'}]->(a) WITH c CREATE (c)-[:TEST_REL]->(c);");
        testCall(db, "MATCH (n:TestNode) WITH collect(n) as nodes CALL apoc.refactor.mergeNodes(nodes, {mergeRels: true, produceSelfRel: false, preserveExistingSelfRels: false}) yield node return node",
                (r) -> assertFalse(((Node) r.get("node")).getRelationships().iterator().hasNext()));
    }

    @Test
    public void testMergeNodeShouldCreateSelfRelationshipsInPreExistingSelfRel() {
        db.executeTransactionally("CREATE (a:TestNode {a:'a'})-[:TEST_REL]->(b:TestNode {a:'b'})-[:TEST_REL]->(c:TestNode {a:'c'})\n" +
                "WITH a, c CREATE (a)-[:TEST_REL]->(a) WITH c CREATE (c)-[:TEST_REL]->(c);");
        testCall(db, "MATCH (n:TestNode) WITH collect(n) as nodes CALL apoc.refactor.mergeNodes(nodes, {mergeRels: true, produceSelfRel: true}) yield node return node",
                (r) -> {
                    Node node = (Node) r.get("node");
                    Iterator<Relationship> relIterator = node.getRelationships().iterator();
                    final String expectedRelType = "TEST_REL";
                    final Relationship firstRel = relIterator.next();
                    assertSelfRel(firstRel, expectedRelType);
                    assertTrue(firstRel.getAllProperties().isEmpty());
                    final Relationship secondRel = relIterator.next();
                    assertSelfRel(secondRel, expectedRelType);
                    assertTrue(secondRel.getAllProperties().isEmpty());
                    assertFalse(relIterator.hasNext());
                });
    }

    @Test
    public void testMergeNodeShouldNotCancelOtherRelsWithSelfRelsTrue() {
        db.executeTransactionally("CREATE (a:A {a:'a'})-[:KNOWS {foo: 'bar'}]->(b:B {a:'b'})-[:KNOWS {baz: 'baa'}]->(c:C {a:'c'})\n" +
                "WITH a,b,c CREATE (a)-[:KNOWS {self: 'rel'}]->(a) WITH a,b,c CREATE (a)-[:KNOWS {one: 'two'}]->(c) WITH c,b CREATE (c)-[:KNOWS {three: 'four'}]->(b);");
        testCall(db, "MATCH (n:A), (m:B) WITH [n,m] as nodes CALL apoc.refactor.mergeNodes(nodes, {mergeRels: true, produceSelfRel: true}) yield node return node",
                (r) -> {
                    Node node = (Node) r.get("node");
                    List<Relationship> relationships = IteratorUtils.toList(node.getRelationships().iterator());

                    assertEquals(4, relationships.size());
                    relationships.sort(Comparator.comparingLong(Relationship::getStartNodeId)
                            .thenComparingLong(Relationship::getEndNodeId));

                    // two A-A rels: the existing one and the new one after merge
                    Relationship firstSelfRel = relationships.get(0);
                    assertEquals("A", firstSelfRel.getStartNode().getLabels().iterator().next().name());
                    assertEquals("A", firstSelfRel.getEndNode().getLabels().iterator().next().name());
                    assertEquals("KNOWS", firstSelfRel.getType().name());
                    assertEquals(Map.of("foo", "bar"), firstSelfRel.getAllProperties());

                    Relationship secondSelfRel = relationships.get(1);
                    assertEquals("A", secondSelfRel.getStartNode().getLabels().iterator().next().name());
                    assertEquals("A", secondSelfRel.getEndNode().getLabels().iterator().next().name());
                    assertEquals("KNOWS", secondSelfRel.getType().name());
                    assertEquals(Map.of("self", "rel"), secondSelfRel.getAllProperties());

                    // two A-C rels created with merge (with combined properties)
                    Relationship firstNotSelfRel = relationships.get(2);
                    assertEquals("A", firstNotSelfRel.getStartNode().getLabels().iterator().next().name());
                    assertEquals("C", firstNotSelfRel.getEndNode().getLabels().iterator().next().name());
                    assertEquals("KNOWS", firstNotSelfRel.getType().name());
                    assertEquals(Map.of("one", "two", "baz", "baa"), firstNotSelfRel.getAllProperties());

                    Relationship secondNotSelfRel = relationships.get(3);
                    assertEquals("C", secondNotSelfRel.getStartNode().getLabels().iterator().next().name());
                    assertEquals("A", secondNotSelfRel.getEndNode().getLabels().iterator().next().name());
                    assertEquals("KNOWS", secondNotSelfRel.getType().name());
                    assertEquals(Map.of("three", "four"), secondNotSelfRel.getAllProperties());
                });
    }

    @Test
    public void testMergeNodeShouldNotCancelOtherRelsWithSelfRelsFalseAndSingleNode() {
        db.executeTransactionally("CREATE (a:A {a:'a'})-[:KNOWS]->(b:B {a:'b'})-[:KNOWS]->(c:C {a:'c'})\n" +
                "WITH a,b,c CREATE (a)-[:KNOWS]->(a) WITH a,b,c CREATE (a)-[:KNOWS]->(c) WITH c,b CREATE (c)-[:KNOWS]->(b);");
        testCall(db, "MATCH (n:A) WITH collect(n) as nodes CALL apoc.refactor.mergeNodes(nodes, {mergeRels: true, produceSelfRel: false}) yield node return node",
                (r) -> {
                    Node node = (Node) r.get("node");
                    List<Relationship> relationships = IteratorUtils.toList(node.getRelationships().iterator());

                    assertEquals(3, relationships.size());
                    relationships.sort(Comparator.comparingLong(Relationship::getStartNodeId)
                            .thenComparingLong(Relationship::getEndNodeId));

                    Relationship firstRel = relationships.get(0);
                    assertEquals("A", firstRel.getStartNode().getLabels().iterator().next().name());
                    assertEquals("A", firstRel.getEndNode().getLabels().iterator().next().name());
                    assertEquals("KNOWS", firstRel.getType().name());

                    Relationship secondRel = relationships.get(1);
                    assertEquals("A", secondRel.getStartNode().getLabels().iterator().next().name());
                    assertEquals("B", secondRel.getEndNode().getLabels().iterator().next().name());
                    assertEquals("KNOWS", secondRel.getType().name());

                    Relationship thirdRel = relationships.get(2);
                    assertEquals("A", thirdRel.getStartNode().getLabels().iterator().next().name());
                    assertEquals("C", thirdRel.getEndNode().getLabels().iterator().next().name());
                    assertEquals("KNOWS", thirdRel.getType().name());
                });
    }

    @Test
    public void testMergeNodeShouldNotCreateSelfRelationshipsWithCircularPath() {
        db.executeTransactionally("CREATE (a:TestNode {a:'a'})-[:TEST_REL]->(b:TestNode {a:'b'})-[:TEST_REL]->(c:TestNode {a:'c'})\n" +
                "WITH a, c CREATE (c)-[:TEST_REL]->(a);");
        testCall(db, "MATCH (n:TestNode) WITH collect(n) as nodes CALL apoc.refactor.mergeNodes(nodes, {mergeRels: true, produceSelfRel: false}) yield node return node",
                (r) -> {
                    Node node = (Node) r.get("node");
                    assertFalse(node.getRelationships().iterator().hasNext());
                });
    }

    @Test
    public void testMergeNodeShouldCreateSelfRelationshipsWithCircularPath() {
        db.executeTransactionally("CREATE (a:TestNode {a:'a'})-[:TEST_REL]->(b:TestNode {a:'b'})-[:TEST_REL]->(c:TestNode {a:'c'})\n" +
                "WITH a, c CREATE (c)-[:TEST_REL]->(a);");
        testCall(db, "MATCH (n:TestNode) WITH collect(n) as nodes CALL apoc.refactor.mergeNodes(nodes, {mergeRels: true, produceSelfRel: true}) yield node return node",
                (r) -> {
                    Node node = (Node) r.get("node");
                    Iterator<Relationship> relIterator = node.getRelationships().iterator();
                    assertSelfRel(relIterator.next(), "TEST_REL");
                    assertFalse(relIterator.hasNext());
                });
    }

    @Test
    public void testMergeNodeShouldCreateSelfRelationshipsWithPathWithOtherRels() {
        db.executeTransactionally("CREATE (a:One)-[:TEST_REL1]->(b:Two)-[:TEST_REL2]->(c:Three)\n" +
                "WITH b, c CREATE (b)-[:ASD]->(q:Four), (b)-[:ZXC]->(w:Five) WITH b, c CREATE (b)-[:QWE]->(c)");
        testCall(db, "match (a:One),(b:Two),(c:Three) with [a,b,c] as nodes CALL apoc.refactor.mergeNodes(nodes, {mergeRels: true}) yield node return node",
                (r) -> {
                    Node node = (Node) r.get("node");
                    List<String> relNodeList = IteratorUtils.toList(node.getRelationships().iterator()).stream().
                            map(i-> i.getType().name()).collect(Collectors.toList());
                    assertThat(relNodeList, Matchers.containsInAnyOrder("ASD", "QWE", "ZXC", "TEST_REL1", "TEST_REL2"));

                    final Relationship relTestRel1 = node.getRelationships(RelationshipType.withName("TEST_REL1")).iterator().next();
                    final Relationship relTestRel2 = node.getRelationships(RelationshipType.withName("TEST_REL2")).iterator().next();
                    final Relationship relQwe = node.getRelationships(RelationshipType.withName("QWE")).iterator().next();
                    assertSelfRel(relTestRel1);
                    assertSelfRel(relTestRel2);
                    assertSelfRel(relQwe);
                });
    }

    @Test
    public void testMergeRelsFalseAndProduceSelfRelFalse() {
        db.executeTransactionally("CREATE (a:A), (b:B) CREATE (a)-[:T]->(b) CREATE (a)-[:T]->(b) CREATE (a)-[:Q]->(a)");
        testCall(db, "MATCH (a:A), (b:B) CALL apoc.refactor.mergeNodes([a,b], {mergeRels: false, produceSelfRel: false}) YIELD node RETURN node",
                (r) -> {
                    Node node = (Node) r.get("node");
                    final List<String> actual = StreamSupport.stream(node.getRelationships().spliterator(), false)
                            .map(Relationship::getType)
                            .map(RelationshipType::name)
                            .sorted()
                            .collect(Collectors.toList());
                    assertEquals(List.of("Q", "T", "T"), actual);
                });
    }

    @Test
    public void testMergeRelsTrueAndProduceSelfRelFalse() {
        db.executeTransactionally("CREATE (a:A), (b:B) CREATE (a)-[:T]->(b) CREATE (a)-[:T]->(b) CREATE (a)-[:T]->(b) CREATE (a)-[:Q]->(a)");
        testCall(db, "MATCH (a:A), (b:B) CALL apoc.refactor.mergeNodes([a,b], {mergeRels: true, produceSelfRel: false}) YIELD node RETURN node",
                (r) -> {
                    Node node = (Node) r.get("node");
                    Iterator<Relationship> relIterator = node.getRelationships().iterator();
                    assertSelfRel(relIterator.next(), "Q");
                    assertFalse(relIterator.hasNext());
                });
    }

    @Test
    public void issue2797WithCloneNodes() {
        issue2797Common(CLONE_NODES_QUERY);
    }

    @Test
    public void issue2797WithExtractNode() {
        db.executeTransactionally("CREATE (:Start)-[r:TO_MOVE {name: 1}]->(:End)");
        issue2797Common(EXTRACT_QUERY);
    }

    @Test
    public void issue2797WithCloneSubgraph() {
        issue2797Common(CLONE_SUBGRAPH_QUERY);
    }
    
    private void issue2797Common(String extractQuery) {
        db.executeTransactionally(("CREATE CONSTRAINT unique_book ON (book:MyBook) ASSERT book.name IS UNIQUE"));

        db.executeTransactionally(("CREATE (n:MyBook {name: 1})"));
        
        testCall(db, extractQuery, r -> {
            final String actualError = (String) r.get("error");
            assertTrue(actualError.contains("already exists with label `MyBook` and property `name` = 1"));
            assertNull(r.get("output"));
        });

        testCall(db, "MATCH (n:MyBook) RETURN properties(n) AS props", 
                r -> {
                    final Map<String, Long> expected = Map.of("name", 1L);
                    assertEquals(expected, r.get("props"));
                });

        db.executeTransactionally("DROP CONSTRAINT unique_book");
        db.executeTransactionally("MATCH (n:MyBook) DELETE n");
    }

    private void assertSelfRel(Relationship next) {
        assertSelfRel(next, null);
    }

    private void assertSelfRel(Relationship next, String expectedRelType) {
        assertTrue(isSelfRel(next));
        if (expectedRelType != null) {
            String actualRelType = next.getType().name();
            assertEquals(expectedRelType, actualRelType);
        }
    }
}

