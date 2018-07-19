package apoc.text;

import apoc.util.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.test.TestGraphDatabaseFactory;

import static apoc.util.TestUtil.testCall;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

public class PhoneticTest {

    private GraphDatabaseService db;

    @Before
    public void setUp() throws Exception {
        db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        TestUtil.registerProcedure(db, Phonetic.class);
    }

    @After
    public void tearDown() {
        db.shutdown();
    }

    @Test
    public void shouldComputeSimpleSoundexEncoding() {
        testCall(db, "CALL apoc.text.phonetic('HellodearUser!')", (row) ->
            assertThat(row.get("value"), equalTo("H436"))
        );
    }

    @Test
    public void shouldComputeSimpleSoundexEncodingOfNull() {
        testCall(db, "CALL apoc.text.phonetic(NULL)", (row) ->
                assertThat(row.get("value"), equalTo(null))
        );
    }

    @Test
    public void shouldComputeEmptySoundexEncodingForTheEmptyString() {
        testCall(db, "CALL apoc.text.phonetic('')", (row) ->
                assertThat(row.get("value"), equalTo(""))
        );
    }

    @Test
    public void shouldComputeSoundexEncodingOfManyWords() {
        testCall(db, "CALL apoc.text.phonetic('Hello, dear User!')", (row) ->
                assertThat(row.get("value"), equalTo("H400D600U260"))
        );
    }


    @Test
    public void shouldComputeSoundexEncodingOfManyWordsEvenIfTheStringContainsSomeExtraChars() {
        testCall(db, "CALL apoc.text.phonetic('  ,Hello,  dear User 5!')", (row) ->
                assertThat(row.get("value"), equalTo("H400D600U260"))
        );
    }

    @Test
    public void shouldComputeWordSoundexEncodingOfNull() {
        testCall(db, "CALL apoc.text.phonetic(NULL)", (row) ->
                assertThat(row.get("value"), equalTo(null))
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
        testCall(db, "CALL apoc.text.doubleMetaphone('Apoc')", (row) ->
                assertThat(row.get("value"), equalTo("APK"))
        );
    }

    @Test
    public void shoudlComputeDoubleMetaphoneOfNull() {
        testCall(db, "CALL apoc.text.doubleMetaphone(NULL)", (row) ->
                assertThat(row.get("value"), equalTo(null))
        );
    }

    @Test
    public void shoudlComputeDoubleMetaphoneForTheEmptyString() {
        testCall(db, "CALL apoc.text.doubleMetaphone('')", (row) ->
                assertThat(row.get("value"), equalTo(null))
        );
    }

    @Test
    public void shouldComputeDoubleMetaphoneOfManyWords() {
        testCall(db, "CALL apoc.text.doubleMetaphone('Hello, dear User!')", (row) ->
                assertThat(row.get("value"), equalTo("HLTRASR"))
        );
    }
}
