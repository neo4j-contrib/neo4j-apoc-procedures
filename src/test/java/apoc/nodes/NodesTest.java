package apoc.nodes;

import apoc.create.Create;
import apoc.util.TestUtil;
import org.junit.*;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static apoc.util.Util.map;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.junit.Assert.*;
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
        TestUtil.registerProcedure(db,Nodes.class, Create.class);
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
        TestUtil.testCall(db, "MATCH (n:Foo) RETURN apoc.node.relationship.types(n) AS value", (r) -> assertEquals(asSet("X","Y"), asSet(((List)r.get("value")).iterator())));
        TestUtil.testCall(db, "MATCH (n:Foo) RETURN apoc.node.relationship.types(n,'X') AS value", (r) -> assertEquals(asList("X"), r.get("value")));
        TestUtil.testCall(db, "MATCH (n:Foo) RETURN apoc.node.relationship.types(n,'X|Z') AS value", (r) -> assertEquals(asList("X"), r.get("value")));
        TestUtil.testCall(db, "MATCH (n:Bar) RETURN apoc.node.relationship.types(n) AS value", (r) -> assertEquals(Collections.emptyList(), r.get("value")));
        TestUtil.testCall(db, "RETURN apoc.node.relationship.types(null) AS value", (r) -> assertEquals(null, r.get("value")));
    }
    @Test
    public void hasRelationhip() throws Exception {
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
    @Ignore("Bug 3.5 org.neo4j.graphdb.NotFoundException: No column named 'xxx' was found. Found: (\"xxx\")")
    public void testId() {
        assertTrue(db.execute("CREATE (f:Foo {foo:'bar'}) RETURN apoc.node.id(f) AS id").<Long>columnAs("id").next() >= 0);
        assertTrue(db.execute("CALL apoc.create.vNode(['Foo'],{foo:'bar'}) YIELD node RETURN apoc.node.id(node) AS id").<Long>columnAs("id").next() < 0);

        assertNull(db.execute("RETURN apoc.node.id(null) AS id").<Long>columnAs("id").next());
    }
    @Test
    @Ignore("Bug 3.5 org.neo4j.graphdb.NotFoundException: No column named 'xxx' was found. Found: (\"xxx\")")
    public void testRelId() {
        assertTrue(db.execute("CREATE (f)-[rel:REL {foo:'bar'}]->(f) RETURN apoc.rel.id(rel) AS id").<Long>columnAs("id").next() >= 0);
        assertTrue(db.execute("CREATE (f) WITH f CALL apoc.create.vRelationship(f,'REL',{foo:'bar'},f) YIELD rel RETURN apoc.rel.id(rel) AS id").<Long>columnAs("id").next() < 0);

        assertNull(db.execute("RETURN apoc.rel.id(null) AS id").<Long>columnAs("id").next());
    }
    @Test
    @Ignore("Bug 3.5 org.neo4j.graphdb.NotFoundException: No column named 'xxx' was found. Found: (\"xxx\")")
    public void testLabels() {
        assertEquals(singletonList("Foo"), db.execute("CREATE (f:Foo {foo:'bar'}) RETURN apoc.node.labels(f) AS labels").columnAs("labels").next());
        assertEquals(singletonList("Foo"), db.execute("CALL apoc.create.vNode(['Foo'],{foo:'bar'}) YIELD node RETURN apoc.node.labels(node) AS labels").columnAs("labels").next());
        assertNull(db.execute("RETURN apoc.node.labels(null) AS labels").columnAs("labels").next());
    }
    @Test
    @Ignore("Bug 3.5 org.neo4j.graphdb.NotFoundException: No column named 'xxx' was found. Found: (\"xxx\")")
    public void testProperties() {
        assertEquals(singletonMap("foo","bar"), db.execute("CREATE (f:Foo {foo:'bar'}) RETURN apoc.any.properties(f) AS props").columnAs("props").next());
        assertEquals(singletonMap("foo","bar"), db.execute("CALL apoc.create.vNode(['Foo'],{foo:'bar'}) YIELD node RETURN apoc.any.properties(node) AS props").columnAs("props").next());

        assertEquals(singletonMap("foo","bar"), db.execute("CREATE (f)-[rel:REL {foo:'bar'}]->(f) RETURN apoc.any.properties(rel) AS props").columnAs("props").next());
        assertEquals(singletonMap("foo","bar"), db.execute("CREATE (f) WITH f CALL apoc.create.vRelationship(f,'REL',{foo:'bar'},f) YIELD rel RETURN apoc.any.properties(rel) AS props").columnAs("props").next());

        assertNull(db.execute("RETURN apoc.any.properties(null) AS props").columnAs("props").next());
    }
    @Test
    @Ignore("Bug 3.5 org.neo4j.graphdb.NotFoundException: No column named 'xxx' was found. Found: (\"xxx\")")
    public void testSubProperties() {
        assertEquals(singletonMap("foo","bar"), db.execute("CREATE (f:Foo {foo:'bar'}) RETURN apoc.any.properties(f,['foo']) AS props").columnAs("props").next());
        assertEquals(emptyMap(), db.execute("CREATE (f:Foo {foo:'bar'}) RETURN apoc.any.properties(f,['bar']) AS props").columnAs("props").next());
        assertEquals(null, db.execute("CREATE (f:Foo {foo:'bar'}) RETURN apoc.any.properties(null,['foo']) AS props").columnAs("props").next());
        assertEquals(singletonMap("foo","bar"), db.execute("CALL apoc.create.vNode(['Foo'],{foo:'bar'}) YIELD node RETURN apoc.any.properties(node,['foo']) AS props").columnAs("props").next());
        assertEquals(emptyMap(), db.execute("CALL apoc.create.vNode(['Foo'],{foo:'bar'}) YIELD node RETURN apoc.any.properties(node,['bar']) AS props").columnAs("props").next());

        assertEquals(singletonMap("foo","bar"), db.execute("CREATE (f)-[rel:REL {foo:'bar'}]->(f) RETURN apoc.any.properties(rel,['foo']) AS props").columnAs("props").next());
        assertEquals(emptyMap(), db.execute("CREATE (f)-[rel:REL {foo:'bar'}]->(f) RETURN apoc.any.properties(rel,['bar']) AS props").columnAs("props").next());
        assertEquals(singletonMap("foo","bar"), db.execute("CREATE (f) WITH f CALL apoc.create.vRelationship(f,'REL',{foo:'bar'},f) YIELD rel RETURN apoc.any.properties(rel,['foo']) AS props").columnAs("props").next());
        assertEquals(emptyMap(), db.execute("CREATE (f) WITH f CALL apoc.create.vRelationship(f,'REL',{foo:'bar'},f) YIELD rel RETURN apoc.any.properties(rel,['bar']) AS props").columnAs("props").next());

        assertNull(db.execute("RETURN apoc.any.properties(null,['foo']) AS props").columnAs("props").next());
    }
    @Test
    @Ignore("Bug 3.5 org.neo4j.graphdb.NotFoundException: No column named 'xxx' was found. Found: (\"xxx\")")
    public void testProperty() {
        assertEquals("bar", db.execute("RETURN apoc.any.property({foo:'bar'},'foo') AS props").columnAs("props").next());
        assertEquals(null, db.execute("RETURN apoc.any.property({foo:'bar'},'bar') AS props").columnAs("props").next());

        assertEquals("bar", db.execute("CREATE (f:Foo {foo:'bar'}) RETURN apoc.any.property(f,'foo') AS props").columnAs("props").next());
        assertEquals(null, db.execute("CREATE (f:Foo {foo:'bar'}) RETURN apoc.any.property(f,'bar') AS props").columnAs("props").next());

        assertEquals("bar", db.execute("CREATE (f)-[r:REL {foo:'bar'}]->(f) RETURN apoc.any.property(r,'foo') AS props").columnAs("props").next());
        assertEquals(null, db.execute("CREATE (f)-[r:REL {foo:'bar'}]->(f) RETURN apoc.any.property(r,'bar') AS props").columnAs("props").next());

        assertEquals("bar", db.execute("CALL apoc.create.vNode(['Foo'],{foo:'bar'}) YIELD node RETURN apoc.any.property(node,'foo') AS props").columnAs("props").next());
        assertEquals(null, db.execute("CALL apoc.create.vNode(['Foo'],{foo:'bar'}) YIELD node RETURN apoc.any.property(node,'bar') AS props").columnAs("props").next());

        assertEquals("bar", db.execute("CREATE (f) WITH f CALL apoc.create.vRelationship(f,'REL',{foo:'bar'},f) YIELD rel RETURN apoc.any.property(rel,'foo') AS props").columnAs("props").next());
        assertEquals(null, db.execute("CREATE (f) WITH f CALL apoc.create.vRelationship(f,'REL',{foo:'bar'},f) YIELD rel RETURN apoc.any.property(rel,'bar') AS props").columnAs("props").next());

        assertNull(db.execute("RETURN apoc.any.property(null,'foo') AS props").columnAs("props").next());
        assertNull(db.execute("RETURN apoc.any.property(null,null) AS props").columnAs("props").next());
    }
    @Test
    @Ignore("Bug 3.5 org.neo4j.graphdb.NotFoundException: No column named 'xxx' was found. Found: (\"xxx\")")
    public void testRelType() {
        assertEquals("REL", db.execute("CREATE (f)-[rel:REL {foo:'bar'}]->(f) RETURN apoc.rel.type(rel) AS type").columnAs("type").next());

        assertEquals("REL", db.execute("CREATE (f) WITH f CALL apoc.create.vRelationship(f,'REL',{foo:'bar'},f) YIELD rel RETURN apoc.rel.type(rel) AS type").columnAs("type").next());

        assertNull(db.execute("RETURN apoc.rel.type(null) AS type").columnAs("type").next());
    }

}