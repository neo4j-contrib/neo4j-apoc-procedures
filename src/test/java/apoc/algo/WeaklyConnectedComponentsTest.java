package apoc.algo;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.test.TestGraphDatabaseFactory;

import apoc.util.TestUtil;

public class WeaklyConnectedComponentsTest {
	
	private static GraphDatabaseService db;
	
	public static final String CC_GRAPH =
            "CREATE (a:Node {name:'A'})"+
            "CREATE (b:Node {name:'B'})"+
            "CREATE (c:Node {name:'C'})-[:LINK]->(d:Node {name:'D'})-[:LINK]->(e:Node {name:'E'})-[:LINK]->(c)  "+   
            "CREATE (f:Node {name:'F'})-[:LINK]->(g:Node {name:'G'})-[:LINK]->(f)  "+
            "CREATE (o:Node {name:'O'})"+
            "CREATE (h:Node {name:'H'})-[:LINK]->(o)-[:LINK]->(h)  "+
            "CREATE (i:Node {name:'I'})-[:LINK]->(o)  "+
            "CREATE (j:Node {name:'J'})-[:LINK]->(o)  "+
            "CREATE (k:Node {name:'K'})-[:LINK]->(o)  "+
            "CREATE (l:Node {name:'L'})-[:LINK]->(o)  "+
            "CREATE (m:Node {name:'M'})-[:LINK]->(o)  "+
            "CREATE (n:Node {name:'N'})-[:LINK]->(o)";

	@Before
	public void setUp() throws Exception {
		db = new TestGraphDatabaseFactory().newImpermanentDatabase();
		TestUtil.registerProcedure(db, WeaklyConnectedComponents.class);
	}

	@After
	public void tearDown() {
		db.shutdown();
	}

	// ==========================================================================================

	@Test
    public void shouldReturnExpectedResultCountWhenUsingWeaklyConnected()
    {
		db.execute(CC_GRAPH).close();
		assertExpected( 5, "CALL apoc.algo.wcc()" + "" );
    }
    
    @Test
    public void shouldReturnExpectedResultWhenUsingWeaklyConnected()
    {
    	db.execute(CC_GRAPH).close();
    	assertExpectedResultOfType( Long.class, "CALL apoc.algo.wcc()" + "" );
    }
    
    private void assertExpected( int expectedResultCount, String query )
    {
        TestUtil.testCallCount( db, query, null,5 );
    }
    

    private void assertExpectedResultOfType( java.lang.Class<?> type , String query )
    {
        TestUtil.testResult( db, query, ( result ) -> {
        	Map<String, Object> next = result.next();
        	Object nodeIds = next.get( "nodeIds");   
        	assertThat( nodeIds, is( instanceOf( List.class ) ) );
        	assertThat( ((List)nodeIds).get(0), is( instanceOf( type ) ) );
        	Object stats = next.get( "stats");
        	assertThat( stats, is( instanceOf( Map.class ) ) );
        } );
    }

}
