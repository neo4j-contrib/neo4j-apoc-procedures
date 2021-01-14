package apoc.util;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;

@RunWith(Parameterized.class)
public class UtilQuoteTest {

    public static GraphDatabaseService db;

    @Parameterized.Parameters
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][] {
                { "abc", true },
                { "_id", true },
                { "some_var", true },
                { "$lock", false },
                { "has$inside", true },
                { "ähhh", true },
                { "rübe", true },
                { "rådhuset", true },
                { "1first", false },
                { "first1", true },
                { "a weird identifier", false },
                { "^n", false },
                { "$$n", false },
                { " var ", false },
                { "foo.bar.baz", false },
        });
    }

    private final String identifier;
    private final boolean shouldAvoidQuote;

    public UtilQuoteTest(String identifier, boolean shouldAvoidQuote) {
        this.identifier = identifier;
        this.shouldAvoidQuote = shouldAvoidQuote;
    }

    @Before
    public void setUp() {
        db = new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder()
                .newGraphDatabase();
    }

    @After
    public void tearDown() {
        db.shutdown();
    }

    @Test
    public void shouldQuoteIfNeededForUsageAsParameterName() {
        db.execute(String.format("CREATE (n:TestNode) SET n.%s = true", Util.quote(identifier))).close();
        // If the query did not fail entirely, did it create the expected property?
        TestUtil.testCallCount(db, String.format("MATCH (n:TestNode) WHERE n.`%s` RETURN id(n)", identifier), null, 1);
    }

    @Test
    public void shouldNotQuoteWhenAvoidQuoteIsTrue() {
        assumeTrue(shouldAvoidQuote);
        assertEquals(Util.quote(identifier), identifier);
    }

}
