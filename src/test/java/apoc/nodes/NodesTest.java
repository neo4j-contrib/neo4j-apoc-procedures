package apoc.nodes;

import apoc.create.Create;
import apoc.result.VirtualNode;
import apoc.result.VirtualRelationship;
import apoc.util.TestUtil;
import apoc.util.Util;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.helpers.collection.Iterables;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static apoc.util.Util.map;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.helpers.collection.Iterators.asSet;

/**
 * @author mh
 * @since 18.08.16
 */
public class NodesTest {

    private GraphDatabaseService db;
    @Before
    public void setUp() throws Exception {
        db = TestUtil.apocGraphDatabaseBuilder().newGraphDatabase();
        TestUtil.registerProcedure(db, Nodes.class, Create.class);
    }

    @After
    public void tearDown() {
        db.shutdown();
    }


    @Test
    public void isDense() throws Exception {
        db.execute("CREATE (f:Foo) CREATE (b:Bar) WITH f UNWIND range(1,100) as id CREATE (f)-[:SELF]->(f)").close();

        TestUtil.testCall(db, "MATCH (n) WITH n, apoc.nodes.isDense(n) as dense " +
                        "WHERE n:Foo AND dense OR n:Bar AND NOT dense RETURN count(*) as c",
                (row) -> assertEquals(2L, row.get("c")));
    }
    @Test
    public void link() throws Exception {
        db.execute("UNWIND range(1,10) as id CREATE (n:Foo {id:id}) WITH collect(n) as nodes call apoc.nodes.link(nodes,'BAR') RETURN size(nodes) as len").close();

        ResourceIterator<Long> it = db.execute("MATCH (n:Foo {id:1})-[r:BAR*9]->() RETURN size(r) as len").columnAs("len");
        assertEquals(9L,(long)it.next());
        it.close();
    }
    @Test
    public void delete() throws Exception {
        db.execute("UNWIND range(1,100) as id CREATE (n:Foo {id:id})-[:X]->(n)").close();
        ResourceIterator<Long> it =  db.execute("MATCH (n:Foo) WITH collect(n) as nodes call apoc.nodes.delete(nodes,1) YIELD value as count RETURN count").columnAs("count");
        long count = it.next();
        it.close();

        assertEquals(100L,count);

        it = db.execute("MATCH (n:Foo) RETURN count(*) as c").columnAs("c");
        assertEquals(0L,(long)it.next());
        it.close();
    }

    @Test
    public void types() throws Exception {
        db.execute("CREATE (f:Foo) CREATE (b:Bar) CREATE (f)-[:Y]->(f) CREATE (f)-[:X]->(f)").close();
        TestUtil.testCall(db, "MATCH (n:Foo) RETURN apoc.node.relationship.types(n) AS value", (r) -> assertEquals(asSet("X","Y"), asSet(((List)r.get("value")).toArray())));
        TestUtil.testCall(db, "MATCH (n:Foo) RETURN apoc.node.relationship.types(n,'X') AS value", (r) -> assertEquals(asList("X"), r.get("value")));
        TestUtil.testCall(db, "MATCH (n:Foo) RETURN apoc.node.relationship.types(n,'X|Z') AS value", (r) -> assertEquals(asList("X"), r.get("value")));
        TestUtil.testCall(db, "MATCH (n:Bar) RETURN apoc.node.relationship.types(n) AS value", (r) -> assertEquals(Collections.emptyList(), r.get("value")));
        TestUtil.testCall(db, "RETURN apoc.node.relationship.types(null) AS value", (r) -> assertEquals(null, r.get("value")));
    }

    @Test
    public void nodesTypes() throws Exception {
        // given
        db.execute("CREATE (f:Foo), (f)-[:Y]->(f), (f)-[:X]->(f)").close();
        db.execute("CREATE (f:Bar), (f)-[:YY]->(f), (f)-[:XX]->(f)").close();

        // when
        TestUtil.testCall(db, "MATCH (n) RETURN apoc.nodes.relationship.types(collect(n)) AS value",
                (result) -> {
                    // then
                    List<Map<String, Object>> list = (List<Map<String, Object>>) result.get("value");
                    assertFalse("value should not be empty", list.isEmpty());
                    list.forEach(map -> {
                        Node node = (Node) map.get("node");
                        List<String> data = (List<String>) map.get("types");
                        if (node.hasLabel(Label.label("Foo"))) {
                            assertEquals(asSet("X", "Y"), asSet(data.iterator()));
                        } else {
                            assertEquals(asSet("XX", "YY"), asSet(data.iterator()));
                        }
                    });
                });
    }

    @Test
    public void nodesHasRelationship() throws Exception {
        // given
        db.execute("CREATE (f:Foo), (f)-[:X]->(f)").close();
        db.execute("CREATE (b:Bar), (b)-[:Y]->(b)").close();

        // when
        TestUtil.testCall(db,"MATCH (n:Foo) RETURN apoc.nodes.relationships.exist(collect(n), 'X|Y') AS value", (result) -> {
            // then
            List<Map<String, Object>> list = (List<Map<String, Object>>) result.get("value");
            assertFalse("value should not be empty", list.isEmpty());
            list.forEach(map -> {
                Map<String, Boolean> data = (Map<String, Boolean>) map.get("exists");
                assertEquals(map("X", true, "Y", false), data);
            });
        });

        TestUtil.testCall(db,"MATCH (n:Bar) RETURN apoc.nodes.relationships.exist(collect(n), 'X|Y') AS value", (result) -> {
            // then
            List<Map<String, Object>> list = (List<Map<String, Object>>) result.get("value");
            assertFalse("value should not be empty", list.isEmpty());
            list.forEach(map -> {
                Map<String, Boolean> data = (Map<String, Boolean>) map.get("exists");
                assertEquals(map("X", false, "Y", true), data);
            });
        });
    }

    @Test
    public void hasRelationship() throws Exception {
        db.execute("CREATE (:Foo)-[:Y]->(:Bar),(n:FooBar) WITH n UNWIND range(1,100) as _ CREATE (n)-[:X]->(n)").close();
        TestUtil.testCall(db,"MATCH (n:Foo) RETURN apoc.node.relationship.exists(n,'Y') AS value",(r)-> assertEquals(true,r.get("value")));
        TestUtil.testCall(db,"MATCH (n:Foo) RETURN apoc.node.relationship.exists(n,'Y>') AS value", (r)-> assertEquals(true,r.get("value")));
        TestUtil.testCall(db,"MATCH (n:Foo) RETURN apoc.node.relationship.exists(n,'<Y') AS value", (r)-> assertEquals(false,r.get("value")));
        TestUtil.testCall(db,"MATCH (n:Foo) RETURN apoc.node.relationship.exists(n,'X') AS value", (r)-> assertEquals(false,r.get("value")));

        TestUtil.testCall(db,"MATCH (n:Bar) RETURN apoc.node.relationship.exists(n,'Y') AS value",(r)-> assertEquals(true,r.get("value")));
        TestUtil.testCall(db,"MATCH (n:Bar) RETURN apoc.node.relationship.exists(n,'Y>') AS value", (r)-> assertEquals(false,r.get("value")));
        TestUtil.testCall(db,"MATCH (n:Bar) RETURN apoc.node.relationship.exists(n,'<Y') AS value", (r)-> assertEquals(true,r.get("value")));
        TestUtil.testCall(db,"MATCH (n:Bar) RETURN apoc.node.relationship.exists(n,'X') AS value", (r)-> assertEquals(false,r.get("value")));

        TestUtil.testCall(db,"MATCH (n:FooBar) RETURN apoc.node.relationship.exists(n,'X') AS value",(r)-> assertEquals(true,r.get("value")));
        TestUtil.testCall(db,"MATCH (n:FooBar) RETURN apoc.node.relationship.exists(n,'X>') AS value", (r)-> assertEquals(true,r.get("value")));
        TestUtil.testCall(db,"MATCH (n:FooBar) RETURN apoc.node.relationship.exists(n,'<X') AS value", (r)-> assertEquals(true,r.get("value")));
        TestUtil.testCall(db,"MATCH (n:FooBar) RETURN apoc.node.relationship.exists(n,'Y') AS value", (r)-> assertEquals(false,r.get("value")));
    }

    @Test
    public void hasRelationships() throws Exception {
        db.execute("CREATE (:Foo)-[:Y]->(:Bar),(n:FooBar) WITH n UNWIND range(1,100) as _ CREATE (n)-[:X]->(n)").close();
        TestUtil.testCall(db,"MATCH (n:Foo) RETURN apoc.node.relationships.exist(n,'Y') AS value",(r)-> assertEquals(map("Y",true),r.get("value")));
        TestUtil.testCall(db,"MATCH (n:Foo) RETURN apoc.node.relationships.exist(n,'Y>') AS value", (r)-> assertEquals(map("Y>",true),r.get("value")));
        TestUtil.testCall(db,"MATCH (n:Foo) RETURN apoc.node.relationships.exist(n,'<Y') AS value", (r)-> assertEquals(map("<Y",false),r.get("value")));
        TestUtil.testCall(db,"MATCH (n:Foo) RETURN apoc.node.relationships.exist(n,'X') AS value", (r)-> assertEquals(map("X",false),r.get("value")));

        TestUtil.testCall(db,"MATCH (n:Bar) RETURN apoc.node.relationships.exist(n,'Y') AS value",(r)-> assertEquals(map("Y",true),r.get("value")));
        TestUtil.testCall(db,"MATCH (n:Bar) RETURN apoc.node.relationships.exist(n,'Y>') AS value", (r)-> assertEquals(map("Y>",false),r.get("value")));
        TestUtil.testCall(db,"MATCH (n:Bar) RETURN apoc.node.relationships.exist(n,'<Y') AS value", (r)-> assertEquals(map("<Y",true),r.get("value")));
        TestUtil.testCall(db,"MATCH (n:Bar) RETURN apoc.node.relationships.exist(n,'X') AS value", (r)-> assertEquals(map("X",false),r.get("value")));

        TestUtil.testCall(db,"MATCH (n:FooBar) RETURN apoc.node.relationships.exist(n,'X') AS value",(r)-> assertEquals(map("X",true    ),r.get("value")));
        TestUtil.testCall(db,"MATCH (n:FooBar) RETURN apoc.node.relationships.exist(n,'X>') AS value", (r)-> assertEquals(map("X>",true),r.get("value")));
        TestUtil.testCall(db,"MATCH (n:FooBar) RETURN apoc.node.relationships.exist(n,'<X') AS value", (r)-> assertEquals(map("<X",true),r.get("value")));
        TestUtil.testCall(db,"MATCH (n:FooBar) RETURN apoc.node.relationships.exist(n,'Y') AS value", (r)-> assertEquals(map("Y",false),r.get("value")));
    }

    @Test
    public void testConnected() throws Exception {
        db.execute("CREATE (st:StartThin),(et:EndThin),(ed:EndDense)").close();
        int relCount = 20;
        for (int rel=0;rel<relCount;rel++) {
            db.execute("MATCH (st:StartThin),(et:EndThin),(ed:EndDense) " +
                            " CREATE (st)-[:REL"+rel+"]->(et) " +
                            " WITH * UNWIND RANGE(1,{count}) AS id CREATE (st)-[:REL"+rel+"]->(ed)",
                    map("count",relCount-rel)).close();
        }

        TestUtil.testCall(db, "MATCH (s:StartThin),(e:EndThin)  RETURN apoc.nodes.connected(s,e) as value", (r) -> assertEquals(true, r.get("value")));
        TestUtil.testCall(db, "MATCH (s:StartThin),(e:EndThin)  RETURN apoc.nodes.connected(s,e,'REL3') as value", (r) -> assertEquals(true, r.get("value")));
        TestUtil.testCall(db, "MATCH (s:StartThin),(e:EndThin)  RETURN apoc.nodes.connected(s,e,'REL10>') as value", (r) -> assertEquals(true, r.get("value")));
        TestUtil.testCall(db, "MATCH (s:StartThin),(e:EndThin)  RETURN apoc.nodes.connected(s,e,'REL20') as value", (r) -> assertEquals(false, r.get("value")));
        TestUtil.testCall(db, "MATCH (s:StartThin),(e:EndThin)  RETURN apoc.nodes.connected(s,e,'REL15>|REL20') as value", (r) -> assertEquals(true, r.get("value")));

        TestUtil.testCall(db, "MATCH (s:StartThin),(e:EndDense) RETURN apoc.nodes.connected(s,e) as value", (r) -> assertEquals(true, r.get("value")));
        TestUtil.testCall(db, "MATCH (s:StartThin),(e:EndDense) RETURN apoc.nodes.connected(s,e,'REL3') as value", (r) -> assertEquals(true, r.get("value")));
        TestUtil.testCall(db, "MATCH (s:StartThin),(e:EndDense) RETURN apoc.nodes.connected(s,e,'REL10>') as value", (r) -> assertEquals(true, r.get("value")));
        TestUtil.testCall(db, "MATCH (s:StartThin),(e:EndDense) RETURN apoc.nodes.connected(s,e,'REL20') as value", (r) -> assertEquals(false, r.get("value")));
        TestUtil.testCall(db, "MATCH (s:StartThin),(e:EndDense) RETURN apoc.nodes.connected(s,e,'REL15>|REL20') as value", (r) -> assertEquals(true, r.get("value")));

        TestUtil.testCall(db, "MATCH (s:StartThin),(e:EndThin)  RETURN apoc.nodes.connected(e,s) as value", (r) -> assertEquals(true, r.get("value")));
        TestUtil.testCall(db, "MATCH (s:StartThin),(e:EndThin)  RETURN apoc.nodes.connected(e,s,'REL3') as value", (r) -> assertEquals(true, r.get("value")));
        TestUtil.testCall(db, "MATCH (s:StartThin),(e:EndThin)  RETURN apoc.nodes.connected(e,s,'REL10<') as value", (r) -> assertEquals(true, r.get("value")));
        TestUtil.testCall(db, "MATCH (s:StartThin),(e:EndThin)  RETURN apoc.nodes.connected(e,s,'REL20') as value", (r) -> assertEquals(false, r.get("value")));
        TestUtil.testCall(db, "MATCH (s:StartThin),(e:EndThin)  RETURN apoc.nodes.connected(e,s,'REL15<|REL20') as value", (r) -> assertEquals(true, r.get("value")));

        TestUtil.testCall(db, "MATCH (s:StartThin),(e:EndDense) RETURN apoc.nodes.connected(e,s) as value", (r) -> assertEquals(true, r.get("value")));
        TestUtil.testCall(db, "MATCH (s:StartThin),(e:EndDense) RETURN apoc.nodes.connected(e,s,'REL3') as value", (r) -> assertEquals(true, r.get("value")));
        TestUtil.testCall(db, "MATCH (s:StartThin),(e:EndDense) RETURN apoc.nodes.connected(e,s,'REL10<') as value", (r) -> assertEquals(true, r.get("value")));
        TestUtil.testCall(db, "MATCH (s:StartThin),(e:EndDense) RETURN apoc.nodes.connected(e,s,'REL20') as value", (r) -> assertEquals(false, r.get("value")));
        TestUtil.testCall(db, "MATCH (s:StartThin),(e:EndDense) RETURN apoc.nodes.connected(e,s,'REL15<|REL20') as value", (r) -> assertEquals(true, r.get("value")));

        // todo inverse e,s then also incoming
    }

    @Test
    public void testDegreeTypeAndDirection() {
        db.execute("CREATE (f:Foo) CREATE (b:Bar) CREATE (f)-[:Y]->(b) CREATE (f)-[:Y]->(b) CREATE (f)-[:X]->(b) CREATE (f)<-[:X]-(b)").close();

        TestUtil.testCall(db, "MATCH (f:Foo),(b:Bar)  RETURN apoc.node.degree(f, '<X') as in, apoc.node.degree(f, 'Y>') as out", (r) -> {
            assertEquals(1l, r.get("in"));
            assertEquals(2l, r.get("out"));
        });

    }

    @Test
    public void testDegreeMultiple() {
        db.execute("CREATE (f:Foo) CREATE (b:Bar) CREATE (f)-[:Y]->(b) CREATE (f)-[:Y]->(b) CREATE (f)-[:X]->(b) CREATE (f)<-[:X]-(b)").close();

        TestUtil.testCall(db, "MATCH (f:Foo),(b:Bar)  RETURN apoc.node.degree(f, '<X|Y') as all", (r) -> {
            assertEquals(3l, r.get("all"));
        });

    }

    @Test
    public void testDegreeTypeOnly() {
        db.execute("CREATE (f:Foo) CREATE (b:Bar) CREATE (f)-[:Y]->(b) CREATE (f)-[:Y]->(b) CREATE (f)-[:X]->(b) CREATE (f)<-[:X]-(b)").close();

        TestUtil.testCall(db, "MATCH (f:Foo),(b:Bar)  RETURN apoc.node.degree(f, 'X') as in, apoc.node.degree(f, 'Y') as out", (r) -> {
            assertEquals(2l, r.get("in"));
            assertEquals(2l, r.get("out"));
        });

    }

    @Test
    public void testDegreeDirectionOnly() {
        db.execute("CREATE (f:Foo) CREATE (b:Bar) CREATE (f)-[:Y]->(b) CREATE (f)-[:X]->(b) CREATE (f)<-[:X]-(b)").close();

        TestUtil.testCall(db, "MATCH (f:Foo),(b:Bar)  RETURN apoc.node.degree(f, '<') as in, apoc.node.degree(f, '>') as out", (r) -> {
            assertEquals(1l, r.get("in"));
            assertEquals(2l, r.get("out"));
        });

    }

    @Test
    public void testDegreeInOutDirectionOnly() {
        db.execute("CREATE (a:Person{name:'test'}) CREATE (b:Person) CREATE (c:Person) CREATE (d:Person) CREATE (a)-[:Rel1]->(b) CREATE (a)-[:Rel1]->(c) CREATE (a)-[:Rel2]->(d) CREATE (a)-[:Rel1]->(b) CREATE (a)<-[:Rel2]-(b) CREATE (a)<-[:Rel2]-(c) CREATE (a)<-[:Rel2]-(d) CREATE (a)<-[:Rel1]-(d)").close();

        TestUtil.testCall(db, "MATCH (a:Person{name:'test'})  RETURN apoc.node.degree.in(a) as in, apoc.node.degree.out(a) as out", (r) -> {
            assertEquals(4l, r.get("in"));
            assertEquals(4l, r.get("out"));
        });

    }

    @Test
    public void testDegreeInOutType() {
        db.execute("CREATE (a:Person{name:'test'}) CREATE (b:Person) CREATE (c:Person) CREATE (d:Person) CREATE (a)-[:Rel1]->(b) CREATE (a)-[:Rel1]->(c) CREATE (a)-[:Rel2]->(d) CREATE (a)-[:Rel1]->(b) CREATE (a)<-[:Rel2]-(b) CREATE (a)<-[:Rel2]-(c) CREATE (a)<-[:Rel2]-(d) CREATE (a)<-[:Rel1]-(d)").close();

        TestUtil.testCall(db, "MATCH (a:Person{name:'test'})  RETURN apoc.node.degree.in(a, 'Rel1') as in1, apoc.node.degree.out(a, 'Rel1') as out1, apoc.node.degree.in(a, 'Rel2') as in2, apoc.node.degree.out(a, 'Rel2') as out2", (r) -> {
            assertEquals(1l, r.get("in1"));
            assertEquals(3l, r.get("out1"));
            assertEquals(3l, r.get("in2"));
            assertEquals(1l, r.get("out2"));
        });

    }

    @Test
    public void testId() {
        assertTrue(db.execute("CREATE (f:Foo {foo:'bar'}) RETURN apoc.node.id(f) AS id").<Long>columnAs("id").next() >= 0);
        assertTrue(db.execute("CALL apoc.create.vNode(['Foo'],{foo:'bar'}) YIELD node RETURN apoc.node.id(node) AS id").<Long>columnAs("id").next() < 0);

        assertNull(db.execute("RETURN apoc.node.id(null) AS id").<Long>columnAs("id").next());
    }
    @Test
    public void testRelId() {
        assertTrue(db.execute("CREATE (f)-[rel:REL {foo:'bar'}]->(f) RETURN apoc.rel.id(rel) AS id").<Long>columnAs("id").next() >= 0);
        assertTrue(db.execute("CREATE (f) WITH f CALL apoc.create.vRelationship(f,'REL',{foo:'bar'},f) YIELD rel RETURN apoc.rel.id(rel) AS id").<Long>columnAs("id").next() < 0);

        assertNull(db.execute("RETURN apoc.rel.id(null) AS id").<Long>columnAs("id").next());
    }
    @Test
    public void testLabels() {
        assertEquals(singletonList("Foo"), db.execute("CREATE (f:Foo {foo:'bar'}) RETURN apoc.node.labels(f) AS labels").columnAs("labels").next());
        assertEquals(singletonList("Foo"), db.execute("CALL apoc.create.vNode(['Foo'],{foo:'bar'}) YIELD node RETURN apoc.node.labels(node) AS labels").columnAs("labels").next());
        assertNull(db.execute("RETURN apoc.node.labels(null) AS labels").columnAs("labels").next());
    }

    @Test
    public void testProperties() {
        assertEquals(singletonMap("foo","bar"), db.execute("CREATE (f:Foo {foo:'bar'}) RETURN apoc.any.properties(f) AS props").columnAs("props").next());
        assertEquals(singletonMap("foo","bar"), db.execute("CALL apoc.create.vNode(['Foo'],{foo:'bar'}) YIELD node RETURN apoc.any.properties(node) AS props").columnAs("props").next());

        assertEquals(singletonMap("foo","bar"), db.execute("CREATE (f)-[rel:REL {foo:'bar'}]->(f) RETURN apoc.any.properties(rel) AS props").columnAs("props").next());
        assertEquals(singletonMap("foo","bar"), db.execute("CREATE (f) WITH f CALL apoc.create.vRelationship(f,'REL',{foo:'bar'},f) YIELD rel RETURN apoc.any.properties(rel) AS props").columnAs("props").next());

        assertNull(db.execute("RETURN apoc.any.properties(null) AS props").columnAs("props").next());
    }

    @Test
    public void testSubProperties() {
        assertEquals(singletonMap("foo","bar"), db.execute("CREATE (f:Foo {foo:'bar'}) RETURN apoc.any.properties(f,['foo']) AS props").columnAs("props").next());
        assertEquals(emptyMap(), db.execute("CREATE (f:Foo {foo:'bar'}) RETURN apoc.any.properties(f,['bar']) AS props").columnAs("props").next());
        assertNull(db.execute("CREATE (f:Foo {foo:'bar'}) RETURN apoc.any.properties(null,['foo']) AS props").columnAs("props").next());
        assertEquals(singletonMap("foo","bar"), db.execute("CALL apoc.create.vNode(['Foo'],{foo:'bar'}) YIELD node RETURN apoc.any.properties(node,['foo']) AS props").columnAs("props").next());
        assertEquals(emptyMap(), db.execute("CALL apoc.create.vNode(['Foo'],{foo:'bar'}) YIELD node RETURN apoc.any.properties(node,['bar']) AS props").columnAs("props").next());

        assertEquals(singletonMap("foo","bar"), db.execute("CREATE (f)-[rel:REL {foo:'bar'}]->(f) RETURN apoc.any.properties(rel,['foo']) AS props").columnAs("props").next());
        assertEquals(emptyMap(), db.execute("CREATE (f)-[rel:REL {foo:'bar'}]->(f) RETURN apoc.any.properties(rel,['bar']) AS props").columnAs("props").next());
        assertEquals(singletonMap("foo","bar"), db.execute("CREATE (f) WITH f CALL apoc.create.vRelationship(f,'REL',{foo:'bar'},f) YIELD rel RETURN apoc.any.properties(rel,['foo']) AS props").columnAs("props").next());
        assertEquals(emptyMap(), db.execute("CREATE (f) WITH f CALL apoc.create.vRelationship(f,'REL',{foo:'bar'},f) YIELD rel RETURN apoc.any.properties(rel,['bar']) AS props").columnAs("props").next());

        assertNull(db.execute("RETURN apoc.any.properties(null,['foo']) AS props").columnAs("props").next());
    }

    @Test
    public void testProperty() {
        assertEquals("bar", db.execute("RETURN apoc.any.property({foo:'bar'},'foo') AS props").columnAs("props").next());
        assertNull(db.execute("RETURN apoc.any.property({foo:'bar'},'bar') AS props").columnAs("props").next());

        assertEquals("bar", db.execute("CREATE (f:Foo {foo:'bar'}) RETURN apoc.any.property(f,'foo') AS props").columnAs("props").next());
        assertNull(db.execute("CREATE (f:Foo {foo:'bar'}) RETURN apoc.any.property(f,'bar') AS props").columnAs("props").next());

        assertEquals("bar", db.execute("CREATE (f)-[r:REL {foo:'bar'}]->(f) RETURN apoc.any.property(r,'foo') AS props").columnAs("props").next());
        assertNull(db.execute("CREATE (f)-[r:REL {foo:'bar'}]->(f) RETURN apoc.any.property(r,'bar') AS props").columnAs("props").next());

        assertEquals("bar", db.execute("CALL apoc.create.vNode(['Foo'],{foo:'bar'}) YIELD node RETURN apoc.any.property(node,'foo') AS props").columnAs("props").next());
        assertNull(db.execute("CALL apoc.create.vNode(['Foo'],{foo:'bar'}) YIELD node RETURN apoc.any.property(node,'bar') AS props").columnAs("props").next());

        assertEquals("bar", db.execute("CREATE (f) WITH f CALL apoc.create.vRelationship(f,'REL',{foo:'bar'},f) YIELD rel RETURN apoc.any.property(rel,'foo') AS props").columnAs("props").next());
        assertNull(db.execute("CREATE (f) WITH f CALL apoc.create.vRelationship(f,'REL',{foo:'bar'},f) YIELD rel RETURN apoc.any.property(rel,'bar') AS props").columnAs("props").next());

        assertNull(db.execute("RETURN apoc.any.property(null,'foo') AS props").columnAs("props").next());
        assertNull(db.execute("RETURN apoc.any.property(null,null) AS props").columnAs("props").next());
    }

    @Test
    public void testRelType() {
        assertEquals("REL", db.execute("CREATE (f)-[rel:REL {foo:'bar'}]->(f) RETURN apoc.rel.type(rel) AS type").columnAs("type").next());

        assertEquals("REL", db.execute("CREATE (f) WITH f CALL apoc.create.vRelationship(f,'REL',{foo:'bar'},f) YIELD rel RETURN apoc.rel.type(rel) AS type").columnAs("type").next());

        assertNull(db.execute("RETURN apoc.rel.type(null) AS type").columnAs("type").next());
    }

    @Test
    public void testMergeSelfRelationship() {
        db.execute("MATCH (n) detach delete (n)");
        db.execute("CREATE (a1:ALabel {name:'a1'})-[:KNOWS]->(b1:BLabel {name:'b1'})");

        Set<Label> label = asSet(label("ALabel"), label("BLabel"));

        TestUtil.testResult(db,
                "MATCH (p:ALabel)-[r:KNOWS]->(c:BLabel) WITH p,c " +
                        "CALL apoc.nodes.collapse([p,c],{mergeVirtualRels:true, selfRel: true, countMerge: true}) yield from, rel, to " +
                        "return from, rel, to", (r) -> {
                    Map<String, Object> map = r.next();

                    assertMerge(map,
                            Util.map("name","b1", "count", 2), label, //FROM
                            Util.map("count", 1), "KNOWS", //REL
                            Util.map("name", "b1", "count", 2), label); //TO
                    assertFalse(r.hasNext());
                });
    }

    @Test
    public void testMergeSelfRelationshipInverted() {
        db.execute("MATCH (n) detach delete (n)");
        db.execute("CREATE (a1:ALabel {name:'a1'})-[:KNOWS]->(b1:BLabel {name:'b1'})");

        Set<Label> label = asSet(label("BLabel"), label("ALabel"));

        TestUtil.testResult(db,
                "MATCH (p:ALabel)-[r:KNOWS]->(c:BLabel) WITH p,c " +
                        "CALL apoc.nodes.collapse([c,p],{mergeVirtualRels:true, selfRel: true, countMerge: true}) yield from, rel, to " +
                        "return from, rel, to", (r) -> {
                    Map<String, Object> map = r.next();
                    assertMerge(map,
                            Util.map("name","a1", "count", 2), label, //FROM
                            Util.map("count", 1), "KNOWS", //REL
                            Util.map("name", "a1", "count", 2), label); //TO
                    assertFalse(r.hasNext());
                });
    }

    @Test
    public void testMergeNotSelfRelationship() {
        db.execute("MATCH (n) detach delete (n)");
        db.execute("CREATE (a1:ALabel {name:'a1'})-[:KNOWS]->(b1:BLabel {name:'b1'})");

        Set<Label> label = asSet(label("ALabel"), label("BLabel"));

        TestUtil.testResult(db,
                "MATCH (p:ALabel)-[r:KNOWS]->(c:BLabel) WITH p,c " +
                        "CALL apoc.nodes.collapse([p,c],{mergeVirtualRels:true, countMerge: true}) yield from, rel, to " +
                        "return from, rel, to", (r) -> {
                    Map<String, Object> map = r.next();

                    assertEquals(Util.map("name","b1", "count", 2), ((VirtualNode)map.get("from")).getAllProperties());
                    assertEquals(label, labelSet((VirtualNode)map.get("from")));
                    assertNull(((VirtualRelationship) map.get("rel")));
                    assertNull(((Node) map.get("to")));
                    assertFalse(r.hasNext());
                   
                });
    }

    @Test
    public void testMergeWithRelationshipDirection() {
        db.execute("MATCH (n) detach delete (n)");
        db.execute("CREATE " +
                "(a1:ALabel {name:'a1'})-[:KNOWS]->(b1:BLabel {name:'b1'})," +
                "(a1)<-[:KNOWS]-(b2:CLabel {name:'c1'})");

        Set<Label> label = asSet(label("ALabel"), label("BLabel"));

        TestUtil.testResult(db,
                "MATCH (p:ALabel)-[r:KNOWS]->(c:BLabel) WITH p,c " +
                        "CALL apoc.nodes.collapse([p,c],{mergeVirtualRels:true, selfRel: true}) yield from, rel, to " +
                        "return from, rel, to", (r) -> {
                    Map<String, Object> map = r.next();

                    assertEquals(Util.map("name","c1"), ((Node)map.get("from")).getAllProperties());
                    assertEquals(asList(label("CLabel")), ((Node)map.get("from")).getLabels());
                    assertEquals(Collections.emptyMap(), ((VirtualRelationship)map.get("rel")).getAllProperties());
                    assertEquals("KNOWS", ((VirtualRelationship)map.get("rel")).getType().name());
                    assertEquals(Util.map("name", "b1", "count", 2), ((Node)map.get("to")).getAllProperties());
                    assertEquals(label, labelSet((Node)map.get("to")));
                    assertTrue(r.hasNext());
                    map = r.next();
                    assertEquals(Util.map("name","b1", "count", 2), ((VirtualNode)map.get("from")).getAllProperties());
                    assertEquals(label, labelSet((VirtualNode)map.get("from")));
                    assertEquals(Util.map("count", 1), ((VirtualRelationship)map.get("rel")).getAllProperties());
                    assertEquals("KNOWS", ((VirtualRelationship)map.get("rel")).getType().name());
                    assertEquals(Util.map("name", "b1", "count", 2), ((VirtualNode)map.get("to")).getAllProperties());
                    assertEquals(label, labelSet((VirtualNode)map.get("to")));
                    assertFalse(r.hasNext());
                });
    }

    @Test
    public void testMergeRelationship() {
        db.execute("MATCH (n) detach delete (n)");
        db.execute("CREATE " +
                "(a1:ALabel {name:'a1'})-[:HAS_REL]->(b1:BLabel {name:'b1'})," +
                "(a2:ALabel {name:'a2'})-[:HAS_REL]->(b1)," +
                "(a4:ALabel {name:'a4'})-[:HAS_REL]->(b4:BLabel {name:'b4'})");

        Set<Label> label = asSet(label("ALabel"));

        TestUtil.testResult(db,
                "MATCH (p:ALabel{name:'a4'}), (p1:ALabel{name:'a2'}), (p2:ALabel{name:'a1'}) WITH p, p1, p2 " +
                        "CALL apoc.nodes.collapse([p, p1, p2],{mergeVirtualRels:true}) yield from, rel, to " +
                        "return from, rel, to", (r) -> {
                    Map<String, Object> map = r.next();
                    assertMerge(map,
                            Util.map("name", "a1", "count", 3), label, //FROM
                            Collections.emptyMap(), "HAS_REL", //REL
                            Util.map("name", "b4"), asSet(label("BLabel"))); //TO
                    assertTrue(r.hasNext());
                    map = r.next();
                    assertMerge(map,
                            Util.map("name", "a1", "count", 3), label, //FROM
                            Util.map("count", 1), "HAS_REL", //REL
                            Util.map("name", "b1"), asSet(label("BLabel"))); //TO
                    assertFalse(r.hasNext());
                });
    }

    @Test
    public void testMergePersonEmployee() {
        db.execute("MATCH (n) detach delete (n)");
        db.execute("CREATE " +
                "(:Person {name:'mike'})-[:LIVES_IN]->(:City{name:'rome'}), " +
                "(:Employee{name:'mike'})-[:WORKS_FOR]->(:Company{name:'Larus'}), " +
                "(:Person {name:'kate'})-[:LIVES_IN]->(:City{name:'london'}), " +
                "(:Employee{name:'kate'})-[:WORKS_FOR]->(:Company{name:'Neo'})");

        Set<Label> label = asSet(label("Collapsed"), label("Person"), label("Employee"));

        TestUtil.testResult(db,
                "MATCH (p:Person)-[r:LIVES_IN]->(c:City), (e:Employee)-[w:WORKS_FOR]->(m:Company) WITH p,r,c,e,w,m WHERE p.name = e.name " +
                        "CALL apoc.nodes.collapse([p,e],{properties:'combine', mergeVirtualRels:true, countMerge: true, collapsedLabel: true}) yield from, rel, to " +
                        "return from, rel, to", (r) -> {
                    Map<String, Object> map = r.next();
                    assertMerge(map,
                            Util.map("name", "mike", "count", 2), label, //FROM
                            Collections.emptyMap(), "LIVES_IN", //REL
                            Util.map("name", "rome"), asSet(label("City"))); //TO
                    assertTrue(r.hasNext());
                    map = r.next();
                    assertMerge(map,
                            Util.map("name", "mike", "count", 2), label, //FROM
                            Collections.emptyMap(), "WORKS_FOR", //REL
                            Util.map("name", "Larus"), asSet(label("Company"))); //TO
                    assertTrue(r.hasNext());
                    map = r.next();
                    assertMerge(map,
                            Util.map("name", "kate","count", 2), label, //FROM
                            Collections.emptyMap(), "LIVES_IN", //REL
                            Util.map("name", "london"), asSet(label("City"))); //TO
                    assertTrue(r.hasNext());
                    map = r.next();
                    assertMerge(map,
                            Util.map("name", "kate", "count", 2), label, //FROM
                            Collections.emptyMap(), "WORKS_FOR", //REL
                            Util.map("name", "Neo"), asSet(label("Company"))); //TO
                    assertFalse(r.hasNext());
                });
    }

    @Test
    public void testMergeVirtualNode() {
        db.execute("CREATE \n" +
                "(p:Person {name: 'John'})-[:LIVES_IN]->(c:City{name:'London'}),\n" +
                "(p1:Person {name: 'Mike'})-[:LIVES_IN]->(c),\n" +
                "(p2:Person {name: 'Kate'})-[:LIVES_IN]->(c),\n" +
                "(p3:Person {name: 'Budd'})-[:LIVES_IN]->(c),\n" +
                "(p4:Person {name: 'Alex'})-[:LIVES_IN]->(c),\n" +
                "(p1)-[:KNOWS]->(p),\n" +
                "(p2)-[:KNOWS]->(p1),\n" +
                "(p2)-[:KNOWS]->(p3),\n" +
                "(p4)-[:KNOWS]->(p3)\n").close();

        Set<Label> label = asSet(label("City"), label("Person"));

        TestUtil.testResult(db, "MATCH (p:Person)-[:LIVES_IN]->(c:City)\n" +
                "WITH c, c + collect(p) as subgraph\n" +
                "CALL apoc.nodes.collapse(subgraph,{mergeVirtualRels:true, countMerge: true}) yield from, rel, to return from, rel, to", null, result -> {
            Map<String, Object> map = result.next();

            assertEquals(Util.map("name","John", "count", 6), ((VirtualNode)map.get("from")).getAllProperties());
            assertEquals(label, labelSet((VirtualNode) map.get("from")));
            assertNull(((VirtualRelationship) map.get("rel")));
            assertNull(((VirtualNode) map.get("to")));
            assertFalse(result.hasNext());
        });
    }

    @Test
    public void testMergeVirtualNodeBOTH() {
        db.execute("CREATE \n" +
                "(p:Person {name: 'John'})-[:LIVES_IN]->(c:City{name:'London'})," +
                "(c)-[:LIVES_IN]->(p)").close();

        Set<Label> label = asSet(label("City"), label("Person"));

        TestUtil.testResult(db, "MATCH (p:Person)-[:LIVES_IN]->(c:City)-[:LIVES_IN]->(b:Person)\n" +
                "CALL apoc.nodes.collapse([c,p,b],{mergeVirtualRels:true, countMerge: true, selfRel: true}) yield from, rel, to return from, rel, to", null, result -> {
            Map<String, Object> map = result.next();

            assertMerge(map,
                    Util.map("name", "John", "count", 2), label, //FROM
                    Util.map("count", 3), "LIVES_IN", //REL
                    Util.map("name", "John", "count", 2), label); //TO
            assertFalse(result.hasNext());
        });
    }

    private static void assertMerge(Map<String, Object> map,
                                    Map<String, Object> fromProperties, Set<Label> fromLabel,
                                    Map<String, Object> relProperties, String relType,
                                    Map<String, Object> toProperties, Set<Label> toLabel
    ) {
        assertEquals(fromProperties, ((VirtualNode)map.get("from")).getAllProperties());
        assertEquals(fromLabel, labelSet((VirtualNode)map.get("from")));
        assertEquals(relProperties, ((VirtualRelationship)map.get("rel")).getAllProperties());
        assertEquals(relType, ((VirtualRelationship)map.get("rel")).getType().name());
        assertEquals(toProperties, ((Node)map.get("to")).getAllProperties());
        assertEquals(toLabel, labelSet((Node)map.get("to")));
    }

    private static Set<Label> labelSet(Node node) {
	   return Iterables.asSet(node.getLabels());
    }
}
