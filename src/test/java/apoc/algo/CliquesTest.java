package apoc.algo;

import apoc.util.TestUtil;
import org.hamcrest.Matcher;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.*;

import static apoc.util.MapUtil.map;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.*;

/**
 * @author mh
 * @since 07.05.16
 */
public class CliquesTest {

    private GraphDatabaseService db;

    @Before
    public void setUp() throws Exception {
        db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        TestUtil.registerProcedure(db, Cliques.class);
    }

    @After
    public void tearDown() {
        db.shutdown();
    }

    private static final String SINGLE_CLIQUE = "CREATE (a {n:'a'}), (b {n:'b'}), (c {n:'c'})" +
            "                       CREATE (a)-[:R]->(b), (a)-[:R]->(c), (b)-[:R]->(c) ";

    private static final String SINGLE_CLIQUE_WITH_EXTRA = "CREATE (a {n:'a'}), (b {n:'b'}), (c {n:'c'}), " +
            " (d {n:'d'}), (e {n:'e'}), (f {n:'f'})" +
            " CREATE (a)-[:R]->(b), (a)-[:R]->(c), (b)-[:R]->(c) ";

    private static final String TWO_CLIQUES = "CREATE (a {n: 'a'}), (b {n:'b'}), (c {n: 'c'}), " +
            " (d {n: 'd'}), (e {n:'e'}), (f {n:'f'})" +
            " CREATE (a)-[:R]->(b), (a)-[:R]->(c), (b)-[:R]->(c), " +
            " (d)-[:R]->(e), (d)-[:R]->(f), (e)-[:R]->(f) ";

    private static final String TWO_CLIQUES_WITH_EXTRA = "CREATE (a {n:'a'}), (b {n:'b'}), (c {n:'c'}), " +
            " (d {n:'d'}), (e {n:'e'}), (f {n:'f'}), " +
            " (g {n:'g'}), (h {n:'h'}), (i {n:'i'}) "  +
            " CREATE (a)-[:R]->(b), (a)-[:R]->(c), (b)-[:R]->(c), " +
            " (d)-[:R]->(e), (d)-[:R]->(f), (e)-[:R]->(f) ";

    private static final String FOUR_CLIQUES_WITH_EXTRA = "CREATE (a {n:'a'}), (b {n:'b'}), (c {n:'c'}), " +
            " (d {n:'d'}), (e {n:'e'}), (f {n:'f'}), " +
            " (g {n:'g'}), (h {n:'h'}), (i {n:'i'}) "  +
            " CREATE (a)-[:R]->(b), (a)-[:R]->(c), (b)-[:R]->(c), " +
            " (d)-[:R]->(e), (d)-[:R]->(f), (e)-[:R]->(f), " +
            " (a)-[:R]->(d), (a)-[:R]->(i)";

    @Test
    public void testCliquesFromEmptyGraphShouldBeEmpty()
    {
        Result result = db.execute( "CALL apoc.algo.cliques(0)" );
        while(result.hasNext())
        {
            fail("An empty graph should not result in any cliques");
        }
    }

    @Test
    public void testCliquesFromGraphWithOneClique()
    {
        db.execute( SINGLE_CLIQUE ).close();
        Result result = db.execute( "CALL apoc.algo.cliques(0) YIELD clique as clique RETURN clique" );

        assertTrue(result.hasNext());
        while(result.hasNext())
        {
            Map<String,Object> next = result.next();
            List clique = ((List)next.get( "clique" ));

            Collection<String> inClique = getStringPropertiesFromList( clique, "n" );
            assertThat(inClique, hasItems( "a", "b", "c" ));
        }
    }

    @Test
    public void testCliquesFromGraphWithOneCliqueAndExtraNodes()
    {
        db.execute( SINGLE_CLIQUE_WITH_EXTRA ).close();
        Result result = db.execute( "CALL apoc.algo.cliques(0) YIELD clique as clique RETURN clique" );

        LinkedList<Matcher> expected = new LinkedList<>(  );
        expected.add( containsInAnyOrder( "a", "b", "c") );
        expected.add( containsInAnyOrder( "d" ) );
        expected.add( containsInAnyOrder( "e" ) );
        expected.add( containsInAnyOrder( "f" ) );

        assertResults(result, expected);

    }

    public void assertResults(Result result, LinkedList<Matcher> expected) {
        assertTrue(result.hasNext());
        while(result.hasNext())
        {
            Map<String,Object> next = result.next();
            List clique = ((List)next.get( "clique" ));

            Collection<String> actual = getStringPropertiesFromList( clique, "n" );
            assertTrue(matchesOneAndOnlyOne(actual, expected));
        }
    }

    @Test
    public void testCliquesFromGraphWithTwoCliques()
    {
        db.execute( TWO_CLIQUES).close();
        Result result = db.execute( "CALL apoc.algo.cliques(0) YIELD clique as clique RETURN clique" );

        LinkedList<Matcher> expected = new LinkedList<>(  );
        expected.add( containsInAnyOrder( "a", "b", "c") );
        expected.add( containsInAnyOrder( "d","e","f" ) );

        assertResults(result, expected);
    }

    @Test
    public void testCliquesFromGraphWithTwoCliquesAndExtraNodes()
    {
        db.execute( TWO_CLIQUES_WITH_EXTRA).close();
        Result result = db.execute( "CALL apoc.algo.cliques(0) YIELD clique RETURN clique" );

        LinkedList<Matcher> expected = new LinkedList<>(  );
        expected.add( containsInAnyOrder( "a", "b", "c") );
        expected.add( containsInAnyOrder( "d","e","f" ) );
        expected.add( containsInAnyOrder( "g" ) );
        expected.add( containsInAnyOrder( "h" ) );
        expected.add( containsInAnyOrder( "i" ) );

        assertResults(result, expected);
    }

    @Test
    public void testCliquesFromSpecifcNode()
    {
        db.execute( FOUR_CLIQUES_WITH_EXTRA).close();
        Result result = db.execute( "MATCH (n {n:{node}}) " +
                "CALL apoc.algo.cliquesWithNode(n, 1) YIELD clique as clique " +
                "RETURN clique", map( "node","a" ) );

        LinkedList<Matcher> expected = new LinkedList<>(  );
        expected.add( containsInAnyOrder( "a", "b", "c") );
        expected.add( containsInAnyOrder( "a", "d") );
        expected.add( containsInAnyOrder( "a", "i") );
        expected.add( containsInAnyOrder( "a") );

        assertResults(result, expected);
    }

    private boolean matchesOneAndOnlyOne( Collection<String> actual, LinkedList<Matcher> expected )
    {
        Matcher matchedMatcher = null;
        for(Matcher matcher : expected)
        {
            if(matcher.matches( actual ))
            {
                if(matchedMatcher != null)
                {
                    fail("Matched multiple expected collections. Actual " + actual
                            + " Found: " + matcher);
                }
                matchedMatcher = matcher;
            }
        }

        if(matchedMatcher == null)
        {
            fail("Did not match any: " + actual);
        }

        expected.remove(matchedMatcher);
        return true;
    }


    private Collection<String> getStringPropertiesFromList(List nodes, String property )
    {
        Collection<String> values = new LinkedList<>();
        try (Transaction tx = db.beginTx()) {
            for (Object node : nodes) {
                values.add(((Node) node).getProperty(property).toString());
            }
            tx.success();
        }
        return values;
    }

}
