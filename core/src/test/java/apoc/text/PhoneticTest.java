package apoc.text;

import apoc.util.TestUtil;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import static apoc.util.TestUtil.testCall;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

public class PhoneticTest {

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();

    @Before
    public void setUp() throws Exception {
        TestUtil.registerProcedure(db, Phonetic.class);
    }

    @Test
    public void shouldComputeSoundexDifference() {
        testCall(db, "CALL apoc.text.phoneticDelta('Hello Mr Rabbit', 'Hello Mr Ribbit')", (row) ->
                assertThat(row.get("delta"), equalTo(4L))
        );
    }

}
