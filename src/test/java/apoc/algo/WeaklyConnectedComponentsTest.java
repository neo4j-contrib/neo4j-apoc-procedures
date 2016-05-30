package apoc.algo;

import apoc.util.TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class WeaklyConnectedComponentsTest
{
    private static GraphDatabaseService db;
    //TODO refactor in multiple execute statements dirty workaround
    public static final String CC_GRAPH =
            "CREATE (:Node:Record { name : 'A' })  "+
            "WITH count(*) as dummy " +
            "CREATE (:Node:Record { name : 'B' })  "+
            "WITH count(*) as dummy " +
            "CREATE (:Node:Record { name : 'C' }) "+
            "WITH count(*) as dummy " +
            "CREATE (:Node:Record { name : 'D' })  "+
            "WITH count(*) as dummy " +
            "CREATE (:Node:Record { name : 'E' })  "+
            "WITH count(*) as dummy " +
            "CREATE (:Node:Record { name : 'F' })  "+
            "WITH count(*) as dummy " +
            "CREATE (:Node:Record { name : 'G' })  "+
            "WITH count(*) as dummy " +
            "CREATE (:Node:Record { name : 'H' })  "+
            "WITH count(*) as dummy " +
            "CREATE (:Node:Record { name : 'I' })  "+
            "WITH count(*) as dummy " +
            "CREATE (:Node:Record { name : 'J' })  "+
            "WITH count(*) as dummy " +
            "CREATE (:Node:Record { name : 'K' })  "+
            "WITH count(*) as dummy " +
            "CREATE (:Node:Record { name : 'L' })  "+
            "WITH count(*) as dummy " +
            "CREATE (:Node:Record { name : 'M' })  "+
            "WITH count(*) as dummy " +
            "CREATE (:Node:Record { name : 'N' })  "+
            "WITH count(*) as dummy " +
            "CREATE (:Node:Record { name : 'Z' })  "+
            "WITH count(*) as dummy " +
            "MATCH (a:Node),(b:Node),(c:Node)  "+
            "WHERE a.name = 'C' AND b.name = 'D' AND c.name = 'E'  "+
            "CREATE (a)-[:LINK]->(b)-[:LINK]->(c)-[:LINK]->(a)  "+   
            "WITH count(*) as dummy " +
            "MATCH (a:Node),(b:Node)  "+
            "WHERE a.name = 'F' AND b.name = 'G'  "+ 
            "CREATE (a)-[:LINK]->(b)-[:LINK]->(a)  "+
            "WITH count(*) as dummy " +
            "MATCH (a:Node),(b:Node)  "+
            "WHERE a.name = 'H' AND b.name = 'Z'   "+
            "CREATE (a)-[:LINK]->(b)-[:LINK]->(a)  "+
            "WITH count(*) as dummy " +
            "MATCH (a:Node),(b:Node)  "+
            "WHERE a.name = 'I' AND b.name = 'Z'  "+ 
            "CREATE (a)-[:LINK]->(b)  "+
            "WITH count(*) as dummy " +
            "MATCH (a:Node),(b:Node)  "+
            "WHERE a.name = 'J' AND b.name = 'Z'   "+
            "CREATE (a)-[:LINK]->(b)  "+
            "WITH count(*) as dummy " +
            "MATCH (a:Node),(b:Node)  "+
            "WHERE a.name = 'K' AND b.name = 'Z'   "+
            "CREATE (a)-[:LINK]->(b)  "+
            "WITH count(*) as dummy " +
            "MATCH (a:Node),(b:Node)  "+
            "WHERE a.name = 'L' AND b.name = 'Z'   "+
            "CREATE (a)-[:LINK]->(b)  "+
            "WITH count(*) as dummy " +
            "MATCH (a:Node),(b:Node)  "+
            "WHERE a.name = 'M' AND b.name = 'Z'   "+
            "CREATE (a)-[:LINK]->(b)  "+
            "WITH count(*) as dummy " +
            "MATCH (a:Node),(b:Node)  "+
            "WHERE a.name = 'N' AND b.name = 'Z'   "+
            "CREATE (a)-[:LINK]->(b)";

    @BeforeClass
    public static void setUp() throws Exception
    {
        db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        TestUtil.registerProcedure( db, WeaklyConnectedComponents.class );
        db.execute( CC_GRAPH ).close();
    }

    @AfterClass
    public static void tearDown()
    {
        db.shutdown();
    }

    // ==========================================================================================

    @Test
    public void shouldReturnExpectedResultCountWhenUsingWeaklyConnected()
    {
        assertExpectedResult( 5L, "CALL apoc.algo.wcc()" + "" );
    }
    

    private void assertExpectedResult( Long expectedResultCount, String query )
    {
        TestUtil.testResult( db, query, ( result ) -> {
            Object value = result.next().get( "value");   
        	assertThat( value, is( instanceOf( Long.class ) ) );
        	assertThat( (Long)value, equalTo( expectedResultCount ) );
        } );
    }

}

