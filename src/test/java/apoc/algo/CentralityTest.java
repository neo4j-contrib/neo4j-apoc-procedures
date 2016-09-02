package apoc.algo;

import apoc.algo.pagerank.PageRankAlgoTest;
import apoc.util.TestUtil;
import org.junit.*;

import java.util.Map;

import org.neo4j.graphdb.*;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CentralityTest
{
    private static GraphDatabaseService db;
    public static final String RANDOM_GRAPH =
            "FOREACH (_ IN range(0,100) | CREATE ()) " +
            "WITH 0.1 AS p " +
            "MATCH (n1),(n2) WITH n1,n2 LIMIT 1000 WHERE rand() < p " +
            "CREATE (n1)-[:TYPE]->(n2)";

    public static final String COMPANIES_QUERY = "CREATE (a:Company {name:'a'})\n" +
            "CREATE (b:Company {name:'b'})\n" +
            "CREATE (c:Company {name:'c'})\n" +
            "CREATE (d:Company {name:'d'})\n" +
            "CREATE (e:Company {name:'e'})\n" +
            "CREATE (f:Company {name:'f'})\n" +
            "CREATE (g:Company {name:'g'})\n" +
            "CREATE (h:Company {name:'h'})\n" +
            "CREATE (i:Company {name:'i'})\n" +
            "CREATE (j:Company {name:'j'})\n" +
            "CREATE (k:Company {name:'k'})\n" +

            "CREATE\n" +
            "  (b)-[:TYPE_1 {score:0.80}]->(c),\n" +
            "  (c)-[:TYPE_1 {score:0.80}]->(b),\n" +
            "  (d)-[:TYPE_1 {score:0.80}]->(a),\n" +
            "  (e)-[:TYPE_1 {score:0.80}]->(b),\n" +
            "  (e)-[:TYPE_1 {score:0.80}]->(d),\n" +
            "  (e)-[:TYPE_1 {score:0.80}]->(f),\n" +
            "  (f)-[:TYPE_1 {score:0.80}]->(b),\n" +
            "  (f)-[:TYPE_1 {score:0.80}]->(e),\n" +
            "  (g)-[:TYPE_2 {score:0.80}]->(b),\n" +
            "  (g)-[:TYPE_2 {score:0.80}]->(e),\n" +
            "  (h)-[:TYPE_2 {score:0.80}]->(b),\n" +
            "  (h)-[:TYPE_2 {score:0.80}]->(e),\n" +
            "  (i)-[:TYPE_2 {score:0.80}]->(b),\n" +
            "  (i)-[:TYPE_2 {score:0.80}]->(e),\n" +
            "  (j)-[:TYPE_2 {score:0.80}]->(e),\n" +
            "  (k)-[:TYPE_2 {score:0.80}]->(e)\n";

    public static final String STAR_GRAPH = "CREATE (a:Company {name:'a'})\n" +
            "CREATE (b:Company {name:'b'})\n" +
            "CREATE (c:Company {name:'c'})\n" +
            "CREATE (d:Company {name:'d'})\n" +
            "CREATE (e:Company {name:'e'})\n" +
            "CREATE (f:Company {name:'f'})\n" +

            "CREATE\n" +
            "  (d)-[:TYPE_1 {score:0.80}]->(f),\n" +
            "  (e)-[:TYPE_1 {score:0.80}]->(f),\n" +
            "  (f)-[:TYPE_1 {score:0.80}]->(a),\n" +
            "  (f)-[:TYPE_1 {score:0.80}]->(b),\n" +
            "  (f)-[:TYPE_1 {score:0.80}]->(c)\n";

    public static double STAR_GRAPH_EXPECTED=6.0;

    public static final String STAR_GRAPH_LABELS = "CREATE (a:Company {name:'a'})\n" +
            "CREATE (b:Emp {name:'b'})\n" +
            "CREATE (c:Emp {name:'c'})\n" +
            "CREATE (d:Emp {name:'d'})\n" +
            "CREATE (e:Emp {name:'e'})\n" +
            "CREATE (m:Company {name:'m'})\n" +
            "CREATE (n:Company {name:'n'})\n" +
            "CREATE (o:Company {name:'o'})\n" +
            "CREATE (p:Company {name:'p'})\n" +
            "CREATE (q:Company {name:'q'})\n" +

            "CREATE\n" +
            "  (o)-[:TYPE_1 {score:0.80}]->(q),\n" +
            "  (p)-[:TYPE_1 {score:0.80}]->(q),\n" +
            "  (q)-[:TYPE_1 {score:0.80}]->(a),\n" +
            "  (q)-[:TYPE_1 {score:0.80}]->(m),\n" +
            "  (q)-[:TYPE_1 {score:0.80}]->(n)\n";

    public static double STAR_GRAPH_LABELS_EXPECTED=6.0;

    public static final String MULTIPLE_SHORTEST_PATH = "CREATE (a:Company {name:'a'})\n" +
            "CREATE (b:Company {name:'b'})\n" +
            "CREATE (c:Company {name:'c'})\n" +
            "CREATE (d:Company {name:'d'})\n" +
            "CREATE (e:Company {name:'e'})\n" +
            "CREATE (f:Company {name:'f'})\n" +
            "CREATE (g:Company {name:'g'})\n" +

            "CREATE\n" +
            "  (a)-[:TYPE_1 {score:0.80}]->(b),\n" +
            "  (a)-[:TYPE_1 {score:0.80}]->(c),\n" +
            "  (a)-[:TYPE_1 {score:0.80}]->(f),\n" +
            "  (c)-[:TYPE_1 {score:0.80}]->(d),\n" +
            "  (b)-[:TYPE_1 {score:0.80}]->(d),\n" +
            "  (d)-[:TYPE_1 {score:0.80}]->(e),\n" +
            "  (f)-[:TYPE_1 {score:0.80}]->(g),\n" +
            "  (g)-[:TYPE_1 {score:0.80}]->(e)\n";

    public static double MULTIPLE_SHORTEST_PATH_EXPECTED=2.66;

    @Before
    public void setUp() throws Exception
    {
        db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        TestUtil.registerProcedure(db, Centrality.class);
    }

    @After
    public void tearDownDb()
    {
        db.shutdown();
    }


    // ==========================================================================================

    @Test
    public void shouldHandleEmptyNodeSetWhenUsingBetweenness()
    {
        db.execute( RANDOM_GRAPH ).close();
        assertExpectedResult( 0, "CALL apoc.algo.betweenness(['TYPE'],[],'BOTH')" + "" );
    }

    @Test
    public void shouldHandleEmptyNodeSetWhenUsingCloseness()
    {
        db.execute( RANDOM_GRAPH ).close();
        assertExpectedResult( 0, "CALL apoc.algo.closeness(['TYPE'],[],'BOTH')" + "" );
    }

    // ==========================================================================================

    @Test
    public void shouldHandleEmptyRelationshipTypeWhenUsingBetweenness()
    {
        db.execute( RANDOM_GRAPH ).close();
        assertExpectedResult( 50, algoQuery( "CALL apoc.algo.betweenness([],nodes,'BOTH')" ) );
    }

    @Test
    public void shouldHandleEmptyRelationshipTypeUsingCloseness()
    {
        db.execute( RANDOM_GRAPH ).close();
        assertExpectedResult( 50, algoQuery( "CALL apoc.algo.closeness([],nodes,'BOTH')" ) );
    }

    // ==========================================================================================

    @Test
    public void shouldProvideSameResultUsingEmptyRelationshipTypeOrSpecifyAllTypesWhenUsingBetweenness()
    {
        db.execute( RANDOM_GRAPH ).close();
        assertResultsAreEqual( algoQuery( "CALL apoc.algo.betweenness([],nodes,'BOTH')" ),
                algoQuery( "CALL apoc.algo.betweenness(['TYPE'],nodes,'BOTH')" ) );
    }

    @Test
    public void shouldProvideSameResultUsingEmptyRelationshipTypeOrSpecifyAllTypesWhenUsingCloseness()
    {
        db.execute( RANDOM_GRAPH ).close();
        assertResultsAreEqual( algoQuery( "CALL apoc.algo.closeness([],nodes,'BOTH')" ),
                algoQuery( "CALL apoc.algo.closeness(['TYPE'],nodes,'BOTH')" ) );
    }


    // ==========================================================================================

    @Test
    public void shouldHandleRelationshipTypesThatDoesNotExistWhenUsingBetweenness()
    {
        db.execute( RANDOM_GRAPH ).close();
        String algo = "CALL apoc.algo.betweenness(['BAD_VALUE'],nodes,'BOTH')";
        String query = algoQuery( algo );
        assertExpectedResult( 0, query );
    }

    @Test
    public void shouldHandleRelationshipTypesThatDoesNotExistWhenUsingCentrality()
    {
        db.execute( RANDOM_GRAPH ).close();
        assertExpectedResult( 0, algoQuery( "CALL apoc.algo.closeness(['BAD_VALUE'],nodes,'BOTH')" ) );
    }

    // ==========================================================================================

    @Test( expected = RuntimeException.class )
    public void shouldHandleNullNodesWhenUsingBetweenness()
    {
        db.execute( RANDOM_GRAPH ).close();
        assertExpectedResult( 0, algoQuery( "CALL apoc.algo.betweenness([],null,'BOTH')" ) );
    }

    @Test( expected = RuntimeException.class )
    public void shouldHandleNullNodesWhenUsingCentrality()
    {
        db.execute( RANDOM_GRAPH ).close();
        assertExpectedResult( 0, algoQuery( "CALL apoc.algo.closeness([],null,'BOTH')" ) );
    }

    // ==========================================================================================

    @Test( expected = RuntimeException.class )
    public void shouldHandleNullRelationshipTypesWhenUsingBetweenness()
    {
        db.execute( RANDOM_GRAPH ).close();
        assertExpectedResult( 0, algoQuery( "CALL apoc.algo.betweenness(null,nodes,'BOTH')" ) );
    }

    @Test( expected = RuntimeException.class )
    public void shouldHandleNullRelationshipTypesWhenUsingCentrality()
    {
        db.execute( RANDOM_GRAPH ).close();
        assertExpectedResult( 0, algoQuery( "CALL apoc.algo.closeness(null,nodes,'BOTH')" ) );
    }

    // ==========================================================================================

    @Test( expected = RuntimeException.class )
    public void shouldHandleNullDirectionWhenUsingBetweenness()
    {
        db.execute( RANDOM_GRAPH ).close();
        assertExpectedResult( 0, algoQuery( "CALL apoc.algo.betweenness(null,nodes,'null')" ) );
    }

    @Test( expected = RuntimeException.class )
    public void shouldHandleNullDirectionWhenUsingCentrality()
    {
        db.execute( RANDOM_GRAPH ).close();
        assertExpectedResult( 0, algoQuery( "CALL apoc.algo.closeness(null,nodes,'null')" ) );
    }

    // ==========================================================================================

    @Test
    public void shouldReturnExpectedResultCountWhenUsingBetweennessAllDirections()
    {
        db.execute( RANDOM_GRAPH ).close();
        assertExpectedResult( 50, algoQuery( "CALL apoc.algo.betweenness(['TYPE'],nodes,'OUTGOING')" ) );

        assertExpectedResult( 50, algoQuery( "CALL apoc.algo.betweenness(['TYPE'],nodes,'INCOMING')" ) );

        assertExpectedResult( 50, algoQuery( "CALL apoc.algo.betweenness(['TYPE'],nodes,'BOTH')" ) );
    }

    @Test
    public void shouldReturnExpectedResultCountWhenUsingClosenessOutgoingAllDirections()
    {
        db.execute( RANDOM_GRAPH ).close();
        assertExpectedResult( 50, algoQuery( "CALL apoc.algo.closeness(['TYPE'],nodes,'OUTGOING')" ) );

        assertExpectedResult( 50, algoQuery( "CALL apoc.algo.closeness(['TYPE'],nodes,'INCOMING')" ) );

        assertExpectedResult( 50, algoQuery( "CALL apoc.algo.closeness(['TYPE'],nodes,'BOTH')" ) );
    }

// ==========================================================================================

    @Test( expected = RuntimeException.class )
    public void shouldHandleInvalidDirectionWhenUsingBetweenness()
    {
        db.execute( RANDOM_GRAPH ).close();
        assertExpectedResult( 0, algoQuery( "CALL apoc.algo.betweenness(null,nodes,'INVAlid')" ) );
    }

    @Test( expected = RuntimeException.class )
    public void shouldHandleInvalidDirectionWhenUsingCentrality()
    {
        db.execute( RANDOM_GRAPH ).close();
        assertExpectedResult( 0, algoQuery( "CALL apoc.algo.closeness(null,nodes,' ')" ) );
    }

    // ==========================================================================================

    @Test
    public void shouldBeCaseInsensitiveForDirectionBetweenness()
    {
        db.execute( RANDOM_GRAPH ).close();
        assertExpectedResult( 50, algoQuery( "CALL apoc.algo.betweenness(['TYPE'],nodes,'InCominG')" ) );

        assertExpectedResult( 50, algoQuery( "CALL apoc.algo.betweenness(['TYPE'],nodes,'ouTGoiNg')" ) );
    }

    @Test
    public void shouldBeCaseInsensitiveForDirectionCloseness()
    {
        db.execute( RANDOM_GRAPH ).close();
        assertExpectedResult( 50, algoQuery( "CALL apoc.algo.closeness(['TYPE'],nodes,'InCominG')" ) );

        assertExpectedResult( 50, algoQuery( "CALL apoc.algo.closeness(['TYPE'],nodes,'ouTGoiNg')" ) );
    }

    // ==========================================================================================


    @Test
    public void shouldHaveExpectedBetweennessResultsForCypher()
    {
        db.execute( STAR_GRAPH ).close();
        Result result = db.execute("CALL apoc.algo.betweennessCypher({write:true})");
        System.out.println(result.resultAsString());
        Result t =  db.execute("MATCH (n) RETURN n.name as name, n.betweenness_centrality as score ORDER BY score DESC LIMIT 1");
        assertTrue( t.hasNext() );
        assertEquals( CentralityTest.STAR_GRAPH_EXPECTED,
                (double)t.next().get("score"), 0.1D );
        assertFalse(t.hasNext() );
        t.close();
    }

    @Test
    public void shouldHaveExpectedBetweennessMultipleSPCypher()
    {
        db.execute( MULTIPLE_SHORTEST_PATH ).close();
        Result result = db.execute("CALL apoc.algo.betweennessCypher({write:true})");
        System.out.println(result.resultAsString());
        Result t =  db.execute("MATCH (n) RETURN n.name as name, n.betweenness_centrality as score ORDER BY score DESC LIMIT 1");
        assertTrue(t.hasNext());
        assertEquals( CentralityTest.MULTIPLE_SHORTEST_PATH_EXPECTED,
                (double)t.next().get("score"), 0.1D );
        assertFalse(t.hasNext() );
        t.close();
    }

    @Test
    public void shouldHaveExpectedBetweennessNodeRemappingForCypher()
    {
        db.execute( STAR_GRAPH_LABELS ).close();
        Result result = db.execute("CALL apoc.algo.betweennessCypher({relCypher: \"MATCH (s:Company)-[r]->(t:Company) RETURN id(s) as source, id(t) as target, 1 as weight\"," +
                "write:true})");
        System.out.println(result.resultAsString());
        Result t =  db.execute("MATCH (n:Company) RETURN n.name as name, n.betweenness_centrality as score ORDER BY score DESC LIMIT 1");
        assertTrue( t.hasNext() );
        assertEquals( CentralityTest.STAR_GRAPH_LABELS_EXPECTED,
                (double)t.next().get("score"), 0.1D );
        assertFalse(t.hasNext() );
        t.close();
    }

    public String algoQuery( String algo )
    {
        return "MATCH (n) WITH n LIMIT 50 " +
               "WITH collect(n) AS nodes " +
               algo + " YIELD node, score " +
               "RETURN node, score " +
               "ORDER BY score DESC";
    }

    private void assertResultsAreEqual( String query1, String query2 )
    {
        try ( Transaction ignore = db.beginTx() )
        {
            Result result1 = db.execute( query1 );
            Result result2 = db.execute( query2 );
            while ( result1.hasNext() )
            {
                assertTrue( result2.hasNext() );
                assertThat( result1.next(), equalTo( result2.next() ) );
            }
            assertFalse( result2.hasNext() );
        }
    }

    private void assertExpectedResult( int expectedResultCount, String query )
    {
        TestUtil.testResult( db, query, ( result ) -> {
            for ( int i = 0; i < expectedResultCount; i++ )
            {
                assertThat( result.next().get( "node" ), is( instanceOf( Node.class ) ) );
            }
        } );
    }

    private void assertExpectedResult( int expectedResultCount, String query, double expectedCentralityValue )
    {
        TestUtil.testResult( db, query, ( result ) -> {
            for ( int i = 0; i < expectedResultCount; i++ )
            {
                Map<String,Object> r = result.next();
                assertThat( r.get( "node" ), is( instanceOf( Node.class ) ) );
                assertThat( r.get( "centrality" ), equalTo( expectedCentralityValue ) );
            }
        } );
    }
}

