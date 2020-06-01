package apoc.example;

import apoc.util.TestUtil;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import static org.junit.Assert.assertEquals;

/**
 * @author mh
 * @since 25.05.16
 */
public class ExamplesTest {

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();

    @Before
    public void setUp() throws Exception {
        TestUtil.registerProcedure(db,Examples.class);
    }

    @Test
    public void testMovies() throws Exception {
        TestUtil.testCall(db,"CALL apoc.example.movies", r -> {
            assertEquals("movies.cypher",r.get("file"));
            assertEquals(169L,r.get("nodes"));
            assertEquals(250L,r.get("relationships"));
        });
    }

}
