package apoc.algo;

import apoc.algo.pagerank.PageRankAlgoTest;
import apoc.util.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class PageRankTest
{
    private GraphDatabaseService db;

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
                                                 "  (b)-[:SIMILAR {score:0.80}]->(c),\n" +
                                                 "  (c)-[:SIMILAR {score:0.80}]->(b),\n" +
                                                 "  (d)-[:SIMILAR {score:0.80}]->(a),\n" +
                                                 "  (e)-[:SIMILAR {score:0.80}]->(b),\n" +
                                                 "  (e)-[:SIMILAR {score:0.80}]->(d),\n" +
                                                 "  (e)-[:SIMILAR {score:0.80}]->(f),\n" +
                                                 "  (f)-[:SIMILAR {score:0.80}]->(b),\n" +
                                                 "  (f)-[:SIMILAR {score:0.80}]->(e),\n" +
                                                 "  (g)-[:SIMILAR {score:0.80}]->(b),\n" +
                                                 "  (g)-[:SIMILAR {score:0.80}]->(e),\n" +
                                                 "  (h)-[:SIMILAR {score:0.80}]->(b),\n" +
                                                 "  (h)-[:SIMILAR {score:0.80}]->(e),\n" +
                                                 "  (i)-[:SIMILAR {score:0.80}]->(b),\n" +
                                                 "  (i)-[:SIMILAR {score:0.80}]->(e),\n" +
                                                 "  (j)-[:SIMILAR {score:0.80}]->(e),\n" +
                                                 "  (k)-[:SIMILAR {score:0.80}]->(e)\n";

    public static final String RANDOM_GRAPH =
            "FOREACH (_ IN range(0,100) | CREATE ()) " +
            "WITH 0.1 AS p " +
            "MATCH (n1),(n2) WITH n1,n2 LIMIT 1000 WHERE rand() < p " +
            "CREATE (n1)-[:TYPE]->(n2)";

    @Before
    public void setUp() throws Exception
    {
        db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        TestUtil.registerProcedure( db, Algo.class );
    }

    @After
    public void tearDown()
    {
        db.shutdown();
    }


    @Test
    public void shouldGetPageRankExpectedResult() throws IOException
    {
        db.execute( COMPANIES_QUERY ).close();
        String query = "MATCH (b:Company {name:'b'})\n" +
                       "CALL apoc.algo.pageRankWithIterations(20, [b]) YIELD node, score\n" +
                       "RETURN node.name AS name, score\n" +
                       "ORDER BY score DESC";
        Result result = db.execute( query );
        assertTrue( result.hasNext() );
        Map<String,Object> row = result.next();
        assertFalse( result.hasNext() );
        assertEquals( PageRankAlgoTest.EXPECTED, (double) row.get( "score" ), 0.1D );

    }

    @Test
    public void shouldHandleEmptyNodeSet()
    {
        db.execute( RANDOM_GRAPH ).close();
        assertExpectedResult( 0, "CALL apoc.algo.pageRank([])" + "" );
        assertExpectedResult( 0, "CALL apoc.algo.pageRankWithIterations(10,[])" + "" );
    }

    @Test( expected = RuntimeException.class )
    public void shouldHandleNullNodes()
    {
        db.execute( RANDOM_GRAPH ).close();
        assertExpectedResult( 0, algoQuery( "CALL apoc.algo.pageRank(null)" ) );
        assertExpectedResult( 0, algoQuery( "CALL apoc.algo.pageRankWithIterations(10,null)" ) );
    }

    @Test( expected = RuntimeException.class )
    public void shouldHandleNullIterations()
    {
        db.execute( RANDOM_GRAPH ).close();
        assertExpectedResult( 0, algoQuery( "CALL apoc.algo.pageRankWithIterations(null,nodes)" ) );
    }

    public String algoQuery( String algo )
    {
        return "MATCH (n) WITH n LIMIT 50 " +
               "WITH collect(n) AS nodes " +
               algo + " YIELD node, centrality " +
               "RETURN node, centrality " +
               "ORDER BY centrality DESC";
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
}

