package apoc.index;

import apoc.refactor.GraphRefactoring;
import apoc.util.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.test.TestGraphDatabaseFactory;

import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testCall;
import static org.junit.Assert.*;

/**
 * @author mh
 * @since 25.03.16
 */
public class FulltextIndexTest {

    public static final String MONTH = "2015-12";
    public static final String DATE = MONTH + "-01";
    public static final String TYPE = "CHECKIN";

    public static final String NAME = "name";

    public static final String PERSON = "Person";
    public static final String HIPSTER = "Hipster";
    public static final String JOE = "Joe";
    public static final String AGE = "age";
    public static final String JOE_PATTERN = "(joe:" + PERSON + ":" + HIPSTER + " {" + NAME + ":'" + JOE + "'," + AGE + ":42})";


    public static final String PLACE = "Place";
    public static final String PHILZ = "Philz";
    public static final String PHILZ_PATTERN = "(philz:" + PLACE + " {" + NAME + ":'" + PHILZ + "'})";

    public static final String CHECKIN_PATTERN = JOE_PATTERN + "-[checkin:" + TYPE + " {on:'" + DATE + "'}]->" + PHILZ_PATTERN;
    private GraphDatabaseService db;
    private IndexManager index;

    @Before
    public void setUp() throws Exception {
        db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        index = db.index();
        TestUtil.registerProcedure(db, FulltextIndex.class);
    }

    @After
    public void tearDown() {
        db.shutdown();
    }

    @Test
    public void testNodes() throws Exception {
        createData();
        testCall(db,"CALL apoc.index.nodes('Person', 'name:jo*')",
                (row) -> {
                    Node node = (Node) row.get("node");
                    assertEquals(true, node.hasLabel(Label.label(PERSON)));
                    assertEquals(JOE, node.getProperty(NAME));
                });
    }

    @Test
    public void testRelationships() throws Exception {
        createData();
        testCall(db,"CALL apoc.index.relationships('CHECKIN', 'on:2015-*')",
                (row) -> {
                    Relationship rel = (Relationship) row.get("rel");
                    assertEquals(DATE, rel.getProperty("on"));
                    assertEquals(TYPE, rel.getType().name());
                });
    }

    @Test
    public void testBetween() throws Exception {
        createData();
        testCall(db,"MATCH "+JOE_PATTERN+ ","+PHILZ_PATTERN+" WITH joe,philz CALL apoc.index.between(joe, 'CHECKIN', philz, 'on:2015-*') YIELD rel RETURN *",
                (row) -> {
                    Relationship rel = (Relationship) row.get("rel");
                    assertEquals(DATE, rel.getProperty("on"));
                    assertEquals(TYPE, rel.getType().name());
                });
    }

    @Test
    public void testOut() throws Exception {
        createData();
        testCall(db,"MATCH "+JOE_PATTERN+" WITH joe CALL apoc.index.out(joe, 'CHECKIN','on:2015-*') YIELD node RETURN *",
                (row) -> {
                    Node node = (Node) row.get("node");
                    assertEquals(PHILZ, node.getProperty("name"));
                    assertEquals(true, node.hasLabel(Label.label(PLACE)));
                });
    }

    @Test
    public void testIn() throws Exception {
        createData();
        testCall(db,"MATCH "+PHILZ_PATTERN+" WITH philz CALL apoc.index.in(philz, 'CHECKIN','on:2015-*') YIELD node RETURN *",
                (row) -> {
                    Node node = (Node) row.get("node");
                    assertEquals(JOE, node.getProperty("name"));
                    assertEquals(true, node.hasLabel(Label.label(PERSON)));
                    assertEquals(true, node.hasLabel(Label.label(HIPSTER)));
                });
    }

    private void createData() {
        testCall(db, "CREATE "+CHECKIN_PATTERN+" RETURN *",(row)->{
            Node joe = (Node) row.get("joe");
            db.index().forNodes(PERSON).add(joe,"name",joe.getProperty("name"));
            Node philz = (Node) row.get("philz");
            db.index().forNodes(PLACE).add(philz,"name",philz.getProperty("name"));
            db.index().forRelationships(TYPE).add((Relationship) row.get("checkin"),"on",DATE);
        });
    }

    @Test
    public void testAddNode() throws Exception {
        testCall(db, "CREATE " + JOE_PATTERN + " WITH joe CALL apoc.index.addNode(joe,['" + NAME + "']) RETURN *",(row) -> { });
        try (Transaction tx = db.beginTx()) {
            assertTrue(index.existsForNodes(PERSON));
            assertTrue(index.existsForNodes(HIPSTER));
            assertEquals(JOE, index.forNodes(PERSON).query(NAME, "jo*").getSingle().getProperty(NAME));
            assertNull(index.forNodes(PERSON).query(AGE, "42").getSingle());
            tx.success();
        }
    }
    @Test
    public void testAddNodeToExistingIndex() throws Exception {
        db.execute("CALL apoc.index.forNodes({index},{type:'fulltext',to_lower_case:'true',analyzer:'org.apache.lucene.analysis.standard.StandardAnalyzer' })",map("index","std_index")).close();
        db.execute("CREATE " + JOE_PATTERN + " WITH joe CALL apoc.index.addNodeByLabel({index},joe,['" + NAME + "']) RETURN *",map("index",PERSON)).close();
        try (Transaction tx = db.beginTx()) {
            assertTrue(index.existsForNodes(PERSON));
            assertEquals(JOE, index.forNodes(PERSON).query(NAME, "jo*").getSingle().getProperty(NAME));
            assertNull(index.forNodes(PERSON).query(AGE, "42").getSingle());
            tx.success();
        }
    }

    @Test
    public void testAddNodeByLabel() throws Exception {
        testCall(db, "CREATE " + JOE_PATTERN + " WITH joe CALL apoc.index.addNodeByLabel('" + PERSON + "', joe, ['" + NAME + "']) RETURN *",(row) -> { });
        try (Transaction tx = db.beginTx()) {
            assertFalse(index.existsForNodes(HIPSTER));
            assertEquals(JOE, index.forNodes(PERSON).query(NAME, "jo*").getSingle().getProperty(NAME));
            assertNull(index.forNodes(PERSON).query(AGE, "42").getSingle());
            tx.success();
        }
    }
    @Test
    public void testAddNodeByLabelMultipleProperties() throws Exception {
        testCall(db, "CREATE " + JOE_PATTERN + " WITH joe CALL apoc.index.addNodeByLabel('" + PERSON + "', joe, ['" + NAME + "','" + AGE + "']) RETURN *",(row) -> { });
        try (Transaction tx = db.beginTx()) {
            assertFalse(index.existsForNodes(HIPSTER));
            assertEquals(JOE, index.forNodes(PERSON).query(NAME, "jo*").getSingle().getProperty(NAME));
            assertEquals(42L, index.forNodes(PERSON).query(AGE, "42").getSingle().getProperty(AGE));
            tx.success();
        }
    }

    @Test
    public void testAddRelationship() throws Exception {
        testCall(db, "CREATE " + CHECKIN_PATTERN + " WITH checkin CALL apoc.index.addRelationship(checkin, ['on']) RETURN *",(row) -> { });
        try (Transaction tx = db.beginTx()) {
            Relationship rel = index.forRelationships(TYPE).query("on", MONTH + "-*").getSingle();
            assertEquals(DATE, rel.getProperty("on"));
            tx.success();
        }
    }
}
