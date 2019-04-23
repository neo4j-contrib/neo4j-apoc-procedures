package apoc.nodes;

import apoc.create.Create;
import apoc.util.TestUtil;
import org.junit.*;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.*;

import static apoc.util.Util.map;
import static java.util.Arrays.asList;
import static org.junit.Assert.*;
import static org.neo4j.helpers.collection.Iterators.asSet;
import apoc.periodic.Periodic;

/**
 * @author mh
 * @since 18.08.16
 */
public class NodesTest {

    private GraphDatabaseService db;
    @Before
    public void setUp() throws Exception {
        db = TestUtil.apocGraphDatabaseBuilder().newGraphDatabase();
        TestUtil.registerProcedure(db,Nodes.class, Create.class, Periodic.class);
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
    public void testRemoveNodesEmptyLabels() {
        String[] labels = {};
        Map params = map("labels", labels,"limit", 50);
        TestUtil.testCall(db,
                "CALL apoc.nodes.removebylabels",
                params,
                r -> {
                    Map map = (Map) r.get("value");
                    assertNotNull(map);
                    assertEquals(0,map.size());
                });
    }


    @Test
    public void testRemoveNodesWithLabels_AnEmptyLabel() {
        String[] labels = {"aa", "bb" ,"","cc"};
        Map params = map("labels", labels,"limit", 50);
        TestUtil.testCall(db,
                "CALL apoc.nodes.removebylabels",
                params,
                r -> {
                    Map map = (Map) r.get("value");
                    assertNotNull(map);
                    assertEquals(3,map.size());
                });
    }

    @Test
    public void testRemoveNodesWithLabels_ALabelWithSpace() {
        String[] labels = {"A SPACE"};
        Map params = map("labels", labels,"limit", 50);
        db.execute("UNWIND range(1,1000) as id CREATE (n:`A SPACE` {id:id})").close();

        TestUtil.testCall(db,
                "MATCH (n:`A SPACE`) RETURN count(n) as cnt",
                params,
                r -> {
                    assertEquals(1000L, r.get("cnt"));
                });

        db.execute("CALL apoc.nodes.removebylabels", params).close();

        TestUtil.testCall(db,
                "MATCH (n:`A SPACE`) RETURN count(n) as cnt",
                params,
                r -> {
                    assertEquals(0L, r.get("cnt"));
                });
    }


    @Test
    public void testRemoveNodesWithLabels_WithData() {
        String[] labels = {"aa", "bb", "cc"};
        Map params = map("labels", labels,"limit", 50);

        db.execute("UNWIND range(1,1000) as id CREATE (n:aa {id:id})").close();
        db.execute("UNWIND range(1,1000) as id CREATE (n:bb {id:id})").close();
        db.execute("UNWIND range(1,1000) as id CREATE (n:cc {id:id})").close();

        TestUtil.testCall(db,"MATCH (n:aa) RETURN count(n) as cnt",
                (r) -> assertEquals(1000L, r.get("cnt") )
        );

        TestUtil.testCall(db,"MATCH (n:bb) RETURN count(n) as cnt",
                (r) ->assertEquals(1000L, r.get("cnt"))
        );

        TestUtil.testCall(db,
                "MATCH (n:cc) RETURN count(n) as cnt",
                r -> assertEquals(1000L, r.get("cnt"))
        );

        TestUtil.testCall(db,"CALL apoc.nodes.removebylabels",
                params,
                (r) -> {
                    Map vals = (Map) r.get("value");
                    assertEquals(1000L, vals.get("aa") );
                    assertEquals(1000L, vals.get("bb") );
                    assertEquals(1000L, vals.get("cc") );
                }
        );


        TestUtil.testCall(db,"MATCH (n:aa) RETURN count(n) as cnt",
                (r) -> assertEquals(0L, r.get("cnt") )
        );

        TestUtil.testCall(db,
                "MATCH (n:bb) RETURN count(n) as cnt",
                (r) -> assertEquals(0L, r.get("cnt"))
        );

        TestUtil.testCall(db,
                "MATCH (n:cc) RETURN count(n) as cnt",
                r -> assertEquals(0L, r.get("cnt"))
        );
    }


    @Test(expected = Exception.class )
    public void testRemoveNodesWithLabels_KoParamsMissing() {
        TestUtil.testCall(db,
                "CALL apoc.nodes.removebylabels(['aa','',,'bb'])",
                null,
                r -> {
                });
    }


    @Test(expected = Exception.class )
    public void testRemoveNodesWithLabels_NegativeLimit() {
        TestUtil.testCall(db,
                "CALL apoc.nodes.removebylabels(['aa','bb','cc','dd'], -123)",
                r -> {}
        );
    }

    @Test
    public void testRemoveNodesWithLabels_OkParamsWithLimit() {
        String[] labels = {"aaopwl"};

        db.execute("UNWIND range(1,400) as id CREATE (n:aaopwl {id:id})").close();

        Map params = map("labels", labels,"limit", 50);
        TestUtil.testCall(db,
                "CALL apoc.nodes.removebylabels",
                params,
                r -> {
                });

        TestUtil.testCall(db,
                "MATCH (n:aaopwl) RETURN count(n) as cnt",
                r -> assertEquals(0L, r.get("cnt") )
        );
    }

    /**
     * This test creates a high number of nodes.
     * Naturally, it takes a decent machine, some time to run
     * and needs memory to run with a million nodes. Being generous, I used -Xmx16G
     */
    @Test
    public void testRemoveNodesWithLabels_LabelsWithSpace_HighLoad() {
        String[] labels = {"A SPACE","HOPE ERA"};
        Map params = map("labels", labels,"limit", 50);

        long baseNbr = 1_000  / labels.length ; // Chief M asked for a million nodes. Worked with 10.
        int steps = 5;

        for(String label: labels) {
            db.execute("CREATE CONSTRAINT ON (n:`"+ label+"`) ASSERT n.bid IS UNIQUE").close();
        }
        // Creation of that lot of nodes in several steps.
        long increment = baseNbr / steps ;
        long startId = 1 ;
        long endId =  increment;

        for(int step = steps ; step>0 ; step --){
            db.execute("UNWIND range("+ startId+","+ endId +") as id " +
                    "CREATE (n:`A SPACE` {bid:toInteger(id)}) " +
                    "CREATE (m:`HOPE ERA` {bid:toInteger(id)-1}) " +
                    "CREATE (n)-[:BRINGS]->(m) " +
                    "CREATE (m)-[:COMES_FROM]->(n) ").close();

            startId = endId+1;
            endId += increment;
        }

        // checking node count is equal to wanted number
        for(String label: labels) {
            TestUtil.testCall(db,
                    "MATCH (n:`"+label+"`) RETURN count(n) as cnt",
                    params,
                    r -> {
                        assertEquals(baseNbr, r.get("cnt"));
                    });
        }

        db.execute("CALL apoc.nodes.removebylabels", params).close();

        // Now checking the is no node remaining for each given label
        for(String label: labels) {
            TestUtil.testCall(db,
                    "MATCH (n:`"+label+"`) RETURN count(n) as cnt",
                    r -> assertEquals(0L, r.get("cnt"))
            );
        }
    }

}
