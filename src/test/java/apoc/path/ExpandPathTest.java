package apoc.path;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UnsupportedEncodingException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.TestGraphDatabaseFactory;

import apoc.util.TestUtil;

public class ExpandPathTest {
    private GraphDatabaseService db;

	public ExpandPathTest() throws Exception {
	}  
	
	@Before
    public void setUp() throws Exception {
        db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        TestUtil.registerProcedure(db, PathExplorer.class);
        String movies = getFragment("cremovies.cql");
		String bigbrother = "MATCH (per:Person) MERGE (bb:BigBrother {name : 'Big Brother' })  MERGE (bb)-[:FOLLOWS]->(per)";
		 try (Transaction tx = db.beginTx()) {
			db.execute(movies);
			db.execute(bigbrother);
			tx.success();
		 }
    }

    @After
    public void tearDown() {
        db.shutdown();
    }
		
	
	@Test
	public void testExplorePathRelationshipsTest() throws Throwable {
		assertThat(callProcAndCount ("MATCH (m:Movie {title: \"The Matrix\"}) CALL apoc.path.expand(m,\"ACTED_IN<|PRODUCED>|FOLLOWS\",\"-\",0,2) yield expandedPath as pp return pp" ), equalTo(11));
	}

	@Test
	public void testExplorePathLabelWhiteListTest() throws Throwable {
		assertThat(callProcAndCount ("MATCH (m:Movie {title: \"The Matrix\"}) CALL apoc.path.expand(m,\"ACTED_IN|PRODUCED|FOLLOWS\",\"+Person|Movie\",0,3) yield expandedPath as pp return pp" ), equalTo(59));
	}

	@Test
	public void testExplorePathLabelBlackListTest() throws Throwable {
		assertThat(callProcAndCount ("MATCH (m:Movie {title: \"The Matrix\"})  CALL apoc.path.expand(m,\"\",\"-BigBrother\",0,2) yield expandedPath as pp return pp" ), equalTo(44));
	}
	private String getFragment(String name) {
		InputStream is = getClass().getClassLoader().getResourceAsStream(name);
		final char[] buffer = new char[1024];
		final StringBuilder out = new StringBuilder();
		  try (Reader in = new InputStreamReader(is, "UTF-8")) {
		    for (;;) {
		      int rsz = in.read(buffer, 0, buffer.length);
		      if (rsz < 0)
		        break;
		      out.append(buffer, 0, rsz);
		    }
		  }
		  catch (UnsupportedEncodingException ex) {
		    ex.printStackTrace();
		  }
		  catch (IOException ex) {
		      ex.printStackTrace();
		  }
		  return out.toString();
	}
	private int callProcAndCount(String procCallCypher) {
		int cnt = 0;

		try (Transaction tx = db.beginTx()) {
			  Result result = db.execute( procCallCypher ); 
			  while (result.hasNext()) {
				  result.next();
				  cnt++;
			  }
        }
		return cnt;
	}
}
