package apoc.search;

import static org.junit.Assert.assertEquals;

import java.io.InputStream;
import java.util.Scanner;

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
        String movies = getFragment("cremovies.cql");
		String bigbrother = "MATCH (per:Person) MERGE (bb:BigBrother {name : 'Big Brother' })  MERGE (bb)-[:FOLLOWS]->(per)";
		 try (Transaction tx = db.beginTx()) {
			db.execute(movies);
			db.execute(bigbrother);
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
    	query = "call apoc.search.nodeReduced('{Person: \"name\",Movie: [\"title\",\"tagline\"]}','ENDS WITH','s') yield label as n return count(n) as c";
		TestUtil.testCall(db, query, (row) -> assertEquals(29L,row.get("c")));
    }

    @Test 
    public void testMultiSearchNodeAllReduced() throws Throwable {
    	String query = "call apoc.search.nodeAllReduced('{Person: \"name\",Movie: [\"title\",\"tagline\"]}','CONTAINS','her') yield label as n return count(n) as c";
		TestUtil.testCall(db, query, (row) -> assertEquals(6L,row.get("c")));
    	query = "call apoc.search.nodeAllReduced('{Person: \"name\",Movie: [\"title\",\"tagline\"]}','STARTS WITH','Tom') yield values as n return count(n) as c";
		TestUtil.testCall(db, query, (row) -> assertEquals(4L,row.get("c")));
    	query = "call apoc.search.nodeAllReduced('{Person: \"name\",Movie: [\"title\",\"tagline\"]}','ENDS WITH','s') yield id as n return count(n) as c";
		TestUtil.testCall(db, query, (row) -> assertEquals(29L,row.get("c")));
    }

    
    
    private static String getFragment(String name) {
		InputStream is = ParallelNodeSearchTest.class.getClassLoader().getResourceAsStream(name);
		return new Scanner(is).useDelimiter("\\Z").next();
	}
}
