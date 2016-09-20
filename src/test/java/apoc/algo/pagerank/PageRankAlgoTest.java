package apoc.algo.pagerank;

import apoc.Pools;
import apoc.algo.LabelPropagation;
import apoc.util.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.junit.Assert.assertEquals;

public class PageRankAlgoTest
{
    private GraphDatabaseService db;

    public static final String COMPANY_RESULT_QUERY = "MATCH (c:Company) " +
                                                      "WHERE c.name = {name} " +
                                                      "RETURN id(c) AS id, " +
                                                      "c.pagerank AS pagerank";

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

    public static final double EXPECTED = 2.87711;
    static ExecutorService pool = Pools.DEFAULT;

    @Before
    public void setUp() throws Exception
    {
        db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        TestUtil.registerProcedure( db, LabelPropagation.class );
    }

    @After
    public void tearDown()
    {
        db.shutdown();
    }

    @Test
    public void shouldGetPageRankArrayStorageSPI() throws IOException
    {
        db.execute( COMPANIES_QUERY ).close();
        try (Transaction tx = db.beginTx()) {
            PageRank pageRank = new PageRankArrayStorageParallelSPI(db, pool);
            pageRank.compute(20);
            long id = (long) getEntry("b").get("id");
            assertEquals(EXPECTED, pageRank.getResult(id), 0.1D);
            tx.success();
        }
//        for ( int i = 0; i < pageRank.numberOfNodes(); i++ )
//        {
//            System.out.println( pageRank.getResult( i ) );
//        }
    }

    @Test
    public void shouldGetPageRankArrayStorageSPIWithTypes() throws IOException
    {
        db.execute( COMPANIES_QUERY ).close();
        try (Transaction tx = db.beginTx()) {
            PageRank pageRank = new PageRankArrayStorageParallelSPI(db, pool);
            pageRank.compute(20, RelationshipType.withName("TYPE_1"), RelationshipType.withName("TYPE_2"));
            long id = (long) getEntry("b").get("id");
            assertEquals(EXPECTED, pageRank.getResult(id), 0.1D);
            tx.success();
        }
//        for ( int i = 0; i < pageRank.numberOfNodes(); i++ )
//        {
//            System.out.println( pageRank.getResult( i ) );
//        }
    }

    private Map<String,Object> getEntry( String name )
    {
        try ( Result result = db
                .execute( COMPANY_RESULT_QUERY, Collections.<String,Object>singletonMap( "name", name ) ) )
        {
            return result.next();
        }
    }
}

