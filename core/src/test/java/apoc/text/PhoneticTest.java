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
    public void shouldComputeSimpleSoundexEncoding() {
        testCall(db, "RETURN apoc.text.phonetic('HellodearUser!') as value", (row) ->
            assertThat(row.get("value"), equalTo("H436"))
        );
    }

    @Test
    public void shouldComputeSimpleSoundexEncodingOfNull() {
        testCall(db, "RETURN apoc.text.phonetic(null) as value", (row) ->
                assertThat(row.get("value"), equalTo(null))
        );
    }

    @Test
    public void shouldComputeEmptySoundexEncodingForTheEmptyString() {
        testCall(db, "RETURN apoc.text.phonetic('') as value", (row) ->
                assertThat(row.get("value"), equalTo(""))
        );
    }

    @Test
    public void shouldComputeSoundexEncodingOfManyWords() {
        testCall(db, "RETURN apoc.text.phonetic('Hello, dear User!') as value", (row) ->
                assertThat(row.get("value"), equalTo("H400D600U260"))
        );
    }


    @Test
    public void shouldComputeSoundexEncodingOfManyWordsEvenIfTheStringContainsSomeExtraChars() {
        testCall(db, "RETURN apoc.text.phonetic('  ,Hello,  dear User 5!') as value", (row) ->
                assertThat(row.get("value"), equalTo("H400D600U260"))
        );
    }

    @Test
    public void shouldComputeSoundexDifference() {
        testCall(db, "CALL apoc.text.phoneticDelta('Hello Mr Rabbit', 'Hello Mr Ribbit')", (row) ->
                assertThat(row.get("delta"), equalTo(4L))
        );
    }

    @Test
    public void shoudlComputeDoubleMetaphone() {
        testCall(db, "RETURN apoc.text.doubleMetaphone('Apoc') as value", (row) ->
                assertThat(row.get("value"), equalTo("APK"))
        );
    }

    @Test
    public void shoudlComputeDoubleMetaphoneOfNull() {
        testCall(db, "RETURN apoc.text.doubleMetaphone(NULL) as value", (row) ->
                assertThat(row.get("value"), equalTo(null))
        );
    }

    @Test
    public void shoudlComputeDoubleMetaphoneForTheEmptyString() {
        testCall(db, "RETURN apoc.text.doubleMetaphone('') as value", (row) ->
                assertThat(row.get("value"), equalTo(""))
        );
    }

    @Test
    public void shouldComputeDoubleMetaphoneOfManyWords() {
        testCall(db, "RETURN apoc.text.doubleMetaphone('Hello, dear User!') as value    ", (row) ->
                assertThat(row.get("value"), equalTo("HLTRASR"))
        );
    }
}
