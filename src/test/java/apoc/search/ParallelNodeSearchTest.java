package apoc.search;

import static org.junit.Assert.assertEquals;

import apoc.util.Util;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.TestGraphDatabaseFactory;

import apoc.util.TestUtil;

public class ParallelNodeSearchTest {
    private static GraphDatabaseService db;

    @BeforeClass
    public static void setUp() throws Exception {
        db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        TestUtil.registerProcedure(db, ParallelNodeSearch.class);
        String movies = Util.readResourceFile("movies.cypher");
		 try (Transaction tx = db.beginTx()) {
			db.execute(movies);
			tx.success();
		 }
    }

    @AfterClass
    public static void tearDown() {
        db.shutdown();
    }

    @Test 
    public void testMultiSearchNode() throws Throwable {
    	String query = "call apoc.search.node('{Person: \"name\",Movie: [\"title\",\"tagline\"]}','CONTAINS','her') yield node as n return count(n) as c";
		TestUtil.testCall(db, query, (row) -> assertEquals(6L,row.get("c")));
    	query = "call apoc.search.node('{Person: \"name\",Movie: [\"title\",\"tagline\"]}','STARTS WITH','Tom') yield node as n return count(n) as c";
		TestUtil.testCall(db, query, (row) -> assertEquals(4L,row.get("c")));
    	query = "call apoc.search.node('{Person: \"name\",Movie: [\"title\",\"tagline\"]}','ENDS WITH','s') yield node as n return count(n) as c";
		TestUtil.testCall(db, query, (row) -> assertEquals(29L,row.get("c")));
    }

    @Test 
    public void testMultiSearchNodeAll() throws Throwable {
    	String query = "call apoc.search.nodeAll('{Person: \"name\",Movie: [\"title\",\"tagline\"]}','CONTAINS','her') yield node as n return count(n) as c";
		TestUtil.testCall(db, query, (row) -> assertEquals(6L,row.get("c")));
    	query = "call apoc.search.nodeAll('{Person: \"name\",Movie: [\"title\",\"tagline\"]}','STARTS WITH','Tom') yield node as n return count(n) as c";
		TestUtil.testCall(db, query, (row) -> assertEquals(4L,row.get("c")));
    	query = "call apoc.search.nodeAll('{Person: \"name\",Movie: [\"title\",\"tagline\"]}','ENDS WITH','s') yield node as n return count(n) as c";
		TestUtil.testCall(db, query, (row) -> assertEquals(29L,row.get("c")));
    }
    
    @Test 
    public void testMultiSearchNodeReduced() throws Throwable {
    	String query = "call apoc.search.nodeReduced('{Person: \"name\",Movie: [\"title\",\"tagline\"]}','CONTAINS','her') yield id as n return count(n) as c";
		TestUtil.testCall(db, query, (row) -> assertEquals(6L,row.get("c")));
    	query = "call apoc.search.nodeReduced('{Person: \"name\",Movie: [\"title\",\"tagline\"]}','STARTS WITH','Tom') yield values as n return count(n) as c";
		TestUtil.testCall(db, query, (row) -> assertEquals(4L,row.get("c")));
    	query = "call apoc.search.nodeReduced('{Person: \"name\",Movie: [\"title\",\"tagline\"]}','ENDS WITH','s') yield labels as n return count(n) as c";
		TestUtil.testCall(db, query, (row) -> assertEquals(29L,row.get("c")));
    }

    @Test 
    public void testMultiSearchNodeAllReduced() throws Throwable {
    	String query = "call apoc.search.nodeAllReduced('{Person: \"name\",Movie: [\"title\",\"tagline\"]}','CONTAINS','her') yield labels as n return count(n) as c";
		TestUtil.testCall(db, query, (row) -> assertEquals(6L,row.get("c")));
    	query = "call apoc.search.nodeAllReduced('{Person: \"name\",Movie: [\"title\",\"tagline\"]}','STARTS WITH','Tom') yield values as n return count(n) as c";
		TestUtil.testCall(db, query, (row) -> assertEquals(4L,row.get("c")));
    	query = "call apoc.search.nodeAllReduced('{Person: \"name\",Movie: [\"title\",\"tagline\"]}','ENDS WITH','s') yield id as n return count(n) as c";
		TestUtil.testCall(db, query, (row) -> assertEquals(29L,row.get("c")));
    }
    @Test
    public void testMultiSearchNodeAllReducedMapParam() throws Throwable {
    	String query = "call apoc.search.nodeAllReduced({Person: 'name', Movie: ['title','tagline']},'CONTAINS','her') yield labels as n return count(n) as c";
		TestUtil.testCall(db, query, (row) -> assertEquals(6L,row.get("c")));
    	query = "call apoc.search.nodeAllReduced({Person: 'name', Movie: ['title','tagline']},'STARTS WITH','Tom') yield values as n return count(n) as c";
		TestUtil.testCall(db, query, (row) -> assertEquals(4L,row.get("c")));
    	query = "call apoc.search.nodeAllReduced({Person: 'name', Movie: ['title','tagline']},'ENDS WITH','s') yield id as n return count(n) as c";
		TestUtil.testCall(db, query, (row) -> assertEquals(29L,row.get("c")));
    }
    @Test
    public void testMultiSearchNodeNumberComparison() throws Throwable {
    	String query = "call apoc.search.nodeAllReduced({Person: 'born', Movie: ['released']},'>',2000) yield labels as n return count(n) as c";
		TestUtil.testCall(db, query, (row) -> assertEquals(12L,row.get("c")));
    }

    @Test
    public void testMultiSearchNodeNumberExactComparison() throws Throwable {
    	String query = "call apoc.search.nodeAllReduced({Person: 'born', Movie: ['released']},'=',2000) yield labels as n return count(n) as c";
		TestUtil.testCall(db, query, (row) -> assertEquals(3L,row.get("c")));
    	query = "call apoc.search.nodeAllReduced({Person: 'born', Movie: ['released']},'exact',2000) yield labels as n return count(n) as c";
		TestUtil.testCall(db, query, (row) -> assertEquals(3L,row.get("c")));
    }
}
