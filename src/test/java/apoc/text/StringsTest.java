package apoc.text;

import apoc.util.TestUtil;
import org.junit.*;
import org.junit.rules.ExpectedException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.Arrays;
import java.util.List;

import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testCall;
import static java.util.Arrays.asList;
import static org.junit.Assert.*;

/**
 * @author mh
 * @since 05.05.16
 */
public class StringsTest {
    private static GraphDatabaseService db;

    @Rule
    public ExpectedException thrown= ExpectedException.none();


    @BeforeClass
    public static void setUp() throws Exception
    {
        db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        TestUtil.registerProcedure( db, Strings.class );
    }

    @AfterClass
    public static void tearDown()
    {
        db.shutdown();
    }

    @Test
    public void testReplace() throws Exception {
        String text = "&N[]eo 4 #J-(3.0)  ";
        String regex = "[^a-zA-Z0-9]";
        String replacement = "";
        String expected = "Neo4J30";

        testCall(db,
                "CALL apoc.text.replace({text},{regex},{replacement})",
                map("text",text,"regex",regex,"replacement",replacement),
                row -> assertEquals(expected, row.get("value")));
    }

    @Test
    public void testReplaceAllWithNull() throws Exception {
        String text = "&N[]eo 4 #J-(3.0)  ";
        String regex = "[^a-zA-Z0-9]";
        String replacement = "";
        testCall(db,
                "CALL apoc.text.replace({text},{regex},{replacement})",
                map("text",null,"regex",regex,"replacement",replacement),
                row -> assertEquals(null, row.get("value")));
        testCall(db,
                "CALL apoc.text.replace({text},{regex},{replacement})",
                map("text",text,"regex",null,"replacement",replacement),
                row -> assertEquals(null, row.get("value")));
        testCall(db,
                "CALL apoc.text.replace({text},{regex},{replacement})",
                map("text",text,"regex",regex,"replacement",null),
                row -> assertEquals(null, row.get("value")));
    }

    @Test
    public void testJoin() throws Exception {
        List<String> texts = Arrays.asList("1", "2", "3", "4");
        String delimiter = ",";
        String expected = "1,2,3,4";

        testCall(db,
                "CALL apoc.text.join({texts},{delimiter})",
                map("texts",texts,"delimiter",delimiter),
                row -> assertEquals(expected, row.get("value")));
    }

    @Test
    public void testJoinWithNull() throws Exception {
        List<String> texts = Arrays.asList("Hello", null);
        String delimiter = " ";
        String expected = "Hello null";

        testCall(db,
                "CALL apoc.text.join({texts},{delimiter})",
                map("texts",null,"delimiter",delimiter),
                row -> assertEquals(null, row.get("value")));
        testCall(db,
                "CALL apoc.text.join({texts},{delimiter})",
                map("texts",texts,"delimiter",null),
                row -> assertEquals(null, row.get("value")));
        testCall(db,
                "CALL apoc.text.join({texts},{delimiter})",
                map("texts",texts,"delimiter",delimiter),
                row -> assertEquals(expected, row.get("value")));
    }


    @Test public void testCleanWithNull() throws Exception {
        testCall(db,
                "CALL apoc.text.clean(null)",
                row -> assertEquals(null, row.get("value")));
    }

    @Test public void testCompareCleaned() throws Exception {
        String string1 = "&N[]eo 4 #J-(3.0)  ";
        String string2 = " neo4j-<30";
        testCall(db,
                "CALL apoc.text.compareCleaned({text1},{text2})",
                map("text1",string1,"text2",string2),
                row -> assertEquals(true, row.get("value")));
    }

    @Test public void testCompareCleanedWithNull() throws Exception {
        String string1 = "&N[]eo 4 #J-(3.0)  ";
        String string2 = " neo4j-<30";
        testCall(db,
                "CALL apoc.text.compareCleaned({text1},{text2})",
                map("text1",string1,"text2",null),
                row -> assertEquals(null, row.get("value")));
        testCall(db,
                "CALL apoc.text.compareCleaned({text1},{text2})",
                map("text1",null,"text2",string2),
                row -> assertEquals(null, row.get("value")));
    }

    @Test public void testFilterCleanMatches() throws Exception {
        String string1 = "&N[]eo 4 #J-(3.0)  ";
        String string2 = "&N[]eo 4 #c-(3.0)  ";
        String stringToFind = " neo4j-<30";
        List<String> strings = asList(string1, string2, null);
        String query = "UNWIND {strings} as s " +
                "CALL apoc.text.filterCleanMatches(s, {expected}) " +
                "RETURN s";
        testCall(db, query,
                map("strings", strings,"expected",stringToFind),
                row -> assertEquals(string1, row.get("s")));
    }

    @Test
    public void testCompareCleanedInQuery() throws Exception {
        testCall(db,
                        "CALL apoc.text.clean({a}) YIELD value as clean_a " +
                        "CALL apoc.text.clean({b}) YIELD value as clean_b " +
                        "RETURN clean_a = clean_b as eq",
                map("a","&N[]eo 4 #J-(3.0)  ","b"," [N]e o4/J-[]3-0"),
                row -> assertEquals(true, row.get("eq")));
    }


    // Documentation tests
    // These are here to verify the claims made in string.adoc

    @Test
    public void testDocReplace() throws Exception {
        testCall(db,
                "CALL apoc.text.replace('Hello World!', '[^a-zA-Z]', '')",
                row -> assertEquals("HelloWorld", row.get("value")));
    }

    @Test
    public void testDocJoin() throws Exception {
        testCall(db,
                "CALL apoc.text.join(['Hello', 'World'], ' ')",
                row -> assertEquals("Hello World", row.get("value")));
    }

    @Test
    public void testDocClean() throws Exception {
        testCall(db,
                "CALL apoc.text.clean({text})",
                map("text","Hello World!"),
                row -> assertEquals("helloworld", row.get("value")));
    }

    @Test
    public void testDocCompareCleaned() throws Exception {
        testCall(db,
                "CALL apoc.text.compareCleaned({text1}, {text2})",
                map("text1","Hello World!","text2","_hello-world_"),
                row -> assertEquals(true, row.get("value")));
    }

    @Test
    public void testDocFilterCleanMatches() throws Exception {
        testCall(db,
                "UNWIND ['Hello World!', 'hello worlds'] as text " +
                        "CALL apoc.text.filterCleanMatches(text, 'hello_world') RETURN text",
                row -> assertEquals("Hello World!", row.get("text")));
    }

    @Test
    public void testUrlEncode() {
        testCall(db,
                "CALL apoc.text.urlencode('ab cd=gh&ij?') YIELD value RETURN value",
                row -> assertEquals("ab+cd%3Dgh%26ij%3F", row.get("value"))
        );
    }

    @Test
    public void testUrlDecode() {
        testCall(db,
                "CALL apoc.text.urldecode('ab+cd%3Dgh%26ij%3F') YIELD value RETURN value",
                row -> assertEquals("ab cd=gh&ij?", row.get("value"))
        );
    }

    @Test
    public void testUrlDecodeFailure() {
        thrown.expect(QueryExecutionException.class);
        thrown.expectMessage("Failed to invoke procedure `apoc.text.urldecode`: Caused by: java.lang.IllegalArgumentException: URLDecoder: Incomplete trailing escape (%) pattern");
        testCall(db,
                "CALL apoc.text.urldecode('ab+cd%3Dgh%26ij%3') YIELD value RETURN value",
                row -> assertEquals("ab cd=gh&ij?", row.get("value"))
        );
    }

}
