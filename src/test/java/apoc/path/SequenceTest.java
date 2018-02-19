package apoc.path;

import apoc.util.TestUtil;
import apoc.util.Util;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class SequenceTest {
    private static GraphDatabaseService db;

    public SequenceTest() throws Exception {
    }

    @BeforeClass
    public static void setUp() throws Exception {
        db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        TestUtil.registerProcedure(db, PathExplorer.class);
        String movies = Util.readResourceFile("movies.cypher");
        String additionalLink = "match (p:Person{name:'Nora Ephron'}), (m:Movie{title:'When Harry Met Sally'}) create (p)-[:ACTED_IN]->(m)";
        try (Transaction tx = db.beginTx()) {
            db.execute(movies);
            db.execute(additionalLink);
            tx.success();
        }
    }

    @AfterClass
    public static void tearDown() {
        db.shutdown();
    }

    @Test
    public void testBasicSequence() throws Throwable {
        String query = "MATCH (t:Person {name: 'Tom Hanks'}) CALL apoc.path.expandConfig(t,{sequence:'>Person, ACTED_IN>, Movie, <DIRECTED'}) yield path with distinct last(nodes(path)) as node return collect(node.name) as names";
        TestUtil.testCall(db, query, (row) -> {
            List<String> expectedNames = new ArrayList<>(Arrays.asList("Robert Zemeckis", "Mike Nichols", "Ron Howard", "Frank Darabont", "Tom Tykwer", "Andy Wachowski", "Lana Wachowski", "Tom Hanks", "John Patrick Stanley", "Nora Ephron", "Penny Marshall", "Rob Reiner"));
            List<String> names = (List<String>) row.get("names");
            assertEquals(12l, names.size());
            assertTrue(names.containsAll(expectedNames));
        });
    }

    @Test
    public void testSequenceWithMinLevel() throws Throwable {
        String query = "MATCH (t:Person {name: 'Tom Hanks'}) CALL apoc.path.expandConfig(t,{sequence:'>Person, ACTED_IN>, Movie, <DIRECTED', minLevel:3}) yield path with distinct last(nodes(path)) as node return collect(node.name) as names";
        TestUtil.testCall(db, query, (row) -> {
            List<String> expectedNames = new ArrayList<>(Arrays.asList("Robert Zemeckis", "Mike Nichols", "Ron Howard", "Frank Darabont", "Tom Tykwer", "Andy Wachowski", "Lana Wachowski", "John Patrick Stanley", "Nora Ephron", "Penny Marshall", "Rob Reiner"));
            List<String> names = (List<String>) row.get("names");
            assertEquals(11l, names.size());
            assertTrue(names.containsAll(expectedNames));
        });
    }

    @Test
    public void testSequenceWithMaxLevel() throws Throwable {
        String query = "MATCH (t:Person {name: 'Tom Hanks'}) CALL apoc.path.expandConfig(t,{sequence:'>Person, ACTED_IN>, Movie, <DIRECTED', maxLevel:2}) yield path with distinct last(nodes(path)) as node return collect(node.name) as names";
        TestUtil.testCall(db, query, (row) -> {
            List<String> expectedNames = new ArrayList<>(Arrays.asList("Robert Zemeckis", "Mike Nichols", "Ron Howard", "Frank Darabont", "Tom Tykwer", "Andy Wachowski", "Lana Wachowski", "John Patrick Stanley", "Nora Ephron", "Penny Marshall", "Tom Hanks"));
            List<String> names = (List<String>) row.get("names");
            assertEquals(11l, names.size());
            assertTrue(names.containsAll(expectedNames));
        });
    }

    @Test
    public void testSequenceWhenNotBeginningAtStart() throws Throwable {
        String query = "MATCH (t:Person {name: 'Tom Hanks'}) CALL apoc.path.expandConfig(t,{sequence:'ACTED_IN>, Movie, <DIRECTED, >Person, ACTED_IN>', beginSequenceAtStart:false}) yield path with distinct last(nodes(path)) as node return collect(node.name) as names";
        TestUtil.testCall(db, query, (row) -> {
            List<String> expectedNames = new ArrayList<>(Arrays.asList("Robert Zemeckis", "Mike Nichols", "Ron Howard", "Frank Darabont", "Tom Tykwer", "Andy Wachowski", "Lana Wachowski", "Tom Hanks", "John Patrick Stanley", "Nora Ephron", "Penny Marshall", "Rob Reiner"));
            List<String> names = (List<String>) row.get("names");
            assertEquals(12l, names.size());
            assertTrue(names.containsAll(expectedNames));
        });
    }

    @Test
    public void testExpandWithSequenceIgnoresRelFilter() throws Throwable {
        String query = "MATCH (t:Person {name: 'Tom Hanks'}) CALL apoc.path.expandConfig(t,{sequence:'>Person, ACTED_IN>, Movie, <DIRECTED', relationshipFilter:'NONEXIST'}) yield path with distinct last(nodes(path)) as node return collect(node.name) as names";
        TestUtil.testCall(db, query, (row) -> {
            List<String> expectedNames = new ArrayList<>(Arrays.asList("Robert Zemeckis", "Mike Nichols", "Ron Howard", "Frank Darabont", "Tom Tykwer", "Andy Wachowski", "Lana Wachowski", "Tom Hanks", "John Patrick Stanley", "Nora Ephron", "Penny Marshall", "Rob Reiner"));
            List<String> names = (List<String>) row.get("names");
            assertEquals(12l, names.size());
            assertTrue(names.containsAll(expectedNames));
        });
    }

    @Test
    public void testExpandWithSequenceIgnoresLabelFilter() throws Throwable {
        String query = "MATCH (t:Person {name: 'Tom Hanks'}) CALL apoc.path.expandConfig(t,{sequence:'>Person, ACTED_IN>, Movie, <DIRECTED', labelFilter:'-Person,-Movie'}) yield path with distinct last(nodes(path)) as node return collect(node.name) as names";
        TestUtil.testCall(db, query, (row) -> {
            List<String> expectedNames = new ArrayList<>(Arrays.asList("Robert Zemeckis", "Mike Nichols", "Ron Howard", "Frank Darabont", "Tom Tykwer", "Andy Wachowski", "Lana Wachowski", "Tom Hanks", "John Patrick Stanley", "Nora Ephron", "Penny Marshall", "Rob Reiner"));
            List<String> names = (List<String>) row.get("names");
            assertEquals(12l, names.size());
            assertTrue(names.containsAll(expectedNames));
        });
    }
}
