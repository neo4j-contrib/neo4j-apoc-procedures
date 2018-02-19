package apoc.path;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import apoc.util.Util;
import org.junit.*;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.test.TestGraphDatabaseFactory;

import apoc.util.TestUtil;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class LabelSequenceTest {
    private static GraphDatabaseService db;

    public LabelSequenceTest() throws Exception {
    }

    @BeforeClass
    public static void setUp() throws Exception {
        db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        TestUtil.registerProcedure(db, PathExplorer.class);
        String sequence = "create (s:Start{name:'start'})-[:REL]->(:A{name:'a'})-[:REL]->(:B{name:'b'})-[:REL]->(:A:C{name:'ac'})-[:REL]->(:B:A{name:'ba'})-[:REL]->(:D:A{name:'da'})";
        try (Transaction tx = db.beginTx()) {
            db.execute(sequence);
            tx.success();
        }
    }

    @AfterClass
    public static void tearDown() {
        db.shutdown();
    }



    @Test
    public void testBasicSequence() throws Throwable {
        String query = "MATCH (s:Start {name: 'start'}) CALL apoc.path.subgraphNodes(s,{labelFilter:'A,B', beginSequenceAtStart:false}) yield node return collect(distinct node.name) as nodes";
        TestUtil.testCall(db, query, (row) -> {
            List<String> names = (List<String>) row.get("nodes");
            assertEquals(6L,names.size());
            assertTrue(names.containsAll(Arrays.asList("start", "a", "b", "ac", "ba", "da")));
        });
    }

    @Test
    public void testNoMatchWhenImproperlyStartingSequenceAtStart() throws Throwable {
        String query = "MATCH (s:Start {name: 'start'}) CALL apoc.path.subgraphNodes(s,{labelFilter:'A,B', beginSequenceAtStart:true, filterStartNode:true}) yield node return collect(distinct node.name) as nodes";
        TestUtil.testCall(db, query, (row) -> {
            List<String> names = (List<String>) row.get("nodes");
            assertEquals(0L,names.size());
        });
    }

    @Test
    public void testMatchWhenProperlyStartingSequenceAtStart() throws Throwable {
        String query = "MATCH (s:Start {name: 'start'}) CALL apoc.path.subgraphNodes(s,{labelFilter:'Start,A,B', beginSequenceAtStart:true, filterStartNode:true}) yield node return collect(distinct node.name) as nodes";
        TestUtil.testCall(db, query, (row) -> {
            List<String> names = (List<String>) row.get("nodes");
            assertEquals(3L,names.size());
            assertTrue(names.containsAll(Arrays.asList("start", "a", "b")));
        });
    }

    @Test
    public void testSequenceWithBlacklist() throws Throwable {
        String query = "MATCH (s:Start {name: 'start'}) CALL apoc.path.subgraphNodes(s,{labelFilter:'A|-C,B', beginSequenceAtStart:false}) yield node return collect(distinct node.name) as nodes";
        TestUtil.testCall(db, query, (row) -> {
            List<String> names = (List<String>) row.get("nodes");
            assertEquals(3L, names.size());
            assertTrue(names.containsAll(Arrays.asList("start", "a", "b")));
        });
    }

    @Test
    public void testSequenceWithTerminatorNode() throws Throwable {
        String query = "MATCH (s:Start {name: 'start'}) CALL apoc.path.subgraphNodes(s,{labelFilter:'/A|C,B', beginSequenceAtStart:false}) yield node return collect(distinct node.name) as nodes";
        TestUtil.testCall(db, query, (row) -> {
            List<String> names = (List<String>) row.get("nodes");
            assertEquals(1L, names.size());
            assertTrue(names.containsAll(Arrays.asList("a")));
        });
    }

    @Test
    public void testSequenceWithEndNode() throws Throwable {
        String query = "MATCH (s:Start {name: 'start'}) CALL apoc.path.subgraphNodes(s,{labelFilter:'>A|C,B', beginSequenceAtStart:false}) yield node return collect(distinct node.name) as nodes";
        TestUtil.testCall(db, query, (row) -> {
            List<String> names = (List<String>) row.get("nodes");
            assertEquals(3L, names.size());
            assertTrue(names.containsAll(Arrays.asList("a", "ac", "da")));
        });
    }

    @Test
    public void testSequenceWithEndNodeAndLimit() throws Throwable {
        String query = "MATCH (s:Start {name: 'start'}) CALL apoc.path.subgraphNodes(s,{labelFilter:'>A|C,B', beginSequenceAtStart:false, limit:2}) yield node return collect(distinct node.name) as nodes";
        TestUtil.testCall(db, query, (row) -> {
            List<String> names = (List<String>) row.get("nodes");
            assertEquals(2L, names.size());
            assertTrue(names.containsAll(Arrays.asList("a", "ac")));
        });
    }

    @Test
    public void testSequenceWithTerminatorNodeAsStartNodeWithoutFilteringStartNode() throws Throwable {
        String query = "MATCH (a:A {name: 'a'}) CALL apoc.path.subgraphNodes(a,{labelFilter:'/A, B', filterStartNode:false}) yield node return collect(distinct node.name) as nodes";
        TestUtil.testCall(db, query, (row) -> {
            List<String> names = (List<String>) row.get("nodes");
            assertEquals(1L, names.size());
            assertTrue(names.containsAll(Arrays.asList("ac")));
        });
    }

    @Test
    public void testSequenceWithTerminatorNodeAndEndNode() throws Throwable {
        String query = "MATCH (s:Start {name: 'start'}) CALL apoc.path.subgraphNodes(s,{labelFilter:'>A|C|/A:C, B', beginSequenceAtStart:false}) yield node return collect(distinct node.name) as nodes";
        TestUtil.testCall(db, query, (row) -> {
            List<String> names = (List<String>) row.get("nodes");
            assertEquals(2L, names.size());
            assertTrue(names.containsAll(Arrays.asList("a", "ac")));
        });
    }

    @Test
    public void testSequenceWithTerminatorNodeWhenUsingMinLevel() throws Throwable {
        String query = "MATCH (s:Start {name: 'start'}) CALL apoc.path.expandConfig(s,{labelFilter:'/A, B', beginSequenceAtStart:false, minLevel:3}) yield path return collect(distinct last(nodes(path)).name) as nodes";
        TestUtil.testCall(db, query, (row) -> {
            List<String> names = (List<String>) row.get("nodes");
            assertEquals(1L, names.size());
            assertTrue(names.containsAll(Arrays.asList("ac")));
        });
    }
}
