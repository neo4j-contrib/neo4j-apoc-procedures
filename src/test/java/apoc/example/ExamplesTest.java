package apoc.example;

import apoc.coll.Coll;
import apoc.util.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.junit.Assert.*;

/**
 * @author mh
 * @since 25.05.16
 */
public class ExamplesTest {

    private GraphDatabaseService db;
    @Before
    public void setUp() throws Exception {
        db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        TestUtil.registerProcedure(db,Examples.class);
    }
    @After
    public void tearDown() {
        db.shutdown();
    }

    @Test
    public void testMovies() throws Exception {
        TestUtil.testCall(db,"CALL apoc.example.movies", r -> {
            System.out.println(r);
            assertEquals("movies.cypher",r.get("file"));
            assertEquals(169L,r.get("nodes"));
            assertEquals(250L,r.get("relationships"));
        });
    }

}
