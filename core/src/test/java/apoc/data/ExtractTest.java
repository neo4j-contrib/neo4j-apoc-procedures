package apoc.data;

import apoc.util.TestUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testCall;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;

public class ExtractTest {

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();

    @Before
    public void setUp() throws Exception {
        TestUtil.registerProcedure(db, Extract.class);
    }

    @Test
    public void testQuotedEmail() {
        testCall(db, "RETURN apoc.data.domain('<foo@bar.baz>') AS value",
                row -> Assert.assertThat(row.get("value"), equalTo("bar.baz")));
    }

    @Test
    public void testEmail() {
        testCall(db, "RETURN apoc.data.domain('foo@bar.baz') AS value",
                row -> Assert.assertThat(row.get("value"), equalTo("bar.baz")));
    }

    @Test
    public void testNull() {
        testCall(db, "RETURN apoc.data.domain(null) AS value",
                row -> assertEquals(null, row.get("value")));
    }

    @Test
    public void testBadString() {
        testCall(db, "RETURN apoc.data.domain('asdsgawe4ge') AS value",
                row -> assertEquals(null, row.get("value")));
    }

    @Test
    public void testEmptyString() {
        testCall(db, "RETURN apoc.data.domain('') AS value",
                row -> assertEquals(null, row.get("value")));
    }

    @Test
    public void testUrl() {
        testCall(db, "RETURN apoc.data.domain('http://www.example.com/lots-of-stuff') AS value",
                row -> assertEquals("www.example.com", row.get("value")));
    }

    @Test
    public void testQueryParameter() {
        testCall(db, "RETURN apoc.data.domain($param) AS value",
                map("param", "www.foo.bar/baz"),
                row -> assertEquals("www.foo.bar", row.get("value")));
    }

    @Test
    public void testEmailWithDotsBeforeAt() {
        testCall(db, "RETURN apoc.data.domain('foo.foo@bar.baz') AS value",
                row -> Assert.assertThat(row.get("value"), equalTo("bar.baz")));
    }
}
