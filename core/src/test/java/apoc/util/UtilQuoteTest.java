package apoc.util;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;

@RunWith(Parameterized.class)
public class UtilQuoteTest {

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    @Parameterized.Parameters
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][] {
                { "abc", true },
                { "_id", true },
                { "some_var", true },
                { "$lock", false },
                { "has$inside", false },
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

    @Test
    public void shouldQuoteIfNeededForUsageAsParameterName() {
        db.executeTransactionally(String.format("CREATE (n:TestNode) SET n.%s = true", Util.quote(identifier)));
        // If the query did not fail entirely, did it create the expected property?
        TestUtil.testCallCount(db, String.format("MATCH (n:TestNode) WHERE n.`%s` RETURN id(n)", identifier), 1);
    }

    @Test
    public void shouldNotQuoteWhenAvoidQuoteIsTrue() {
        assumeTrue(shouldAvoidQuote);
        assertEquals(Util.quote(identifier), identifier);
    }

}
