package apoc.text;

import apoc.util.TestUtil;
import org.junit.*;
import org.junit.rules.ExpectedException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testResult;
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
                "RETURN apoc.text.regreplace({text},{regex},{replacement}) AS value",
                map("text",text,"regex",regex,"replacement",replacement),
                row -> assertEquals(expected, row.get("value")));
    }

    @Test
    public void testReplaceAllWithNull() throws Exception {
        String text = "&N[]eo 4 #J-(3.0)  ";
        String regex = "[^a-zA-Z0-9]";
        String replacement = "";
        testCall(db,
                "RETURN apoc.text.regreplace({text},{regex},{replacement}) AS value",
                map("text",null,"regex",regex,"replacement",replacement),
                row -> assertEquals(null, row.get("value")));
        testCall(db,
                "RETURN apoc.text.regreplace({text},{regex},{replacement}) AS value",
                map("text",text,"regex",null,"replacement",replacement),
                row -> assertEquals(null, row.get("value")));
        testCall(db,
                "RETURN apoc.text.regreplace({text},{regex},{replacement}) AS value",
                map("text",text,"regex",regex,"replacement",null),
                row -> assertEquals(null, row.get("value")));
    }

    @Test
    public void testJoin() throws Exception {
        List<String> texts = Arrays.asList("1", "2", "3", "4");
        String delimiter = ",";
        String expected = "1,2,3,4";

        testCall(db,
                "RETURN apoc.text.join({texts},{delimiter}) AS value",
                map("texts",texts,"delimiter",delimiter),
                row -> assertEquals(expected, row.get("value")));
    }

    @Test
    public void testJoinWithNull() throws Exception {
        List<String> texts = Arrays.asList("Hello", null);
        String delimiter = " ";
        String expected = "Hello null";

        testCall(db,
                "RETURN apoc.text.join({texts},{delimiter}) AS value",
                map("texts",null,"delimiter",delimiter),
                row -> assertEquals(null, row.get("value")));
        testCall(db,
                "RETURN apoc.text.join({texts},{delimiter}) AS value",
                map("texts",texts,"delimiter",null),
                row -> assertEquals(null, row.get("value")));
        testCall(db,
                "RETURN apoc.text.join({texts},{delimiter}) AS value",
                map("texts",texts,"delimiter",delimiter),
                row -> assertEquals(expected, row.get("value")));
    }


    @Test public void testCleanWithNull() throws Exception {
        testCall(db,
                "RETURN apoc.text.clean(null) AS value",
                row -> assertEquals(null, row.get("value")));
    }

    @Test public void testCompareCleaned() throws Exception {
        String string1 = "&N[]eo 4 #J-(3.0)  ";
        String string2 = " neo4j-<30";
        testCall(db,
                "RETURN apoc.text.compareCleaned({text1},{text2}) AS value",
                map("text1",string1,"text2",string2),
                row -> assertEquals(true, row.get("value")));
    }

    @Test public void testCompareCleanedWithNull() throws Exception {
        String string1 = "&N[]eo 4 #J-(3.0)  ";
        String string2 = " neo4j-<30";
        testCall(db,
                "RETURN apoc.text.compareCleaned({text1},{text2}) AS value",
                map("text1",string1,"text2",null),
                row -> assertEquals(false, row.get("value")));
        testCall(db,
                "RETURN apoc.text.compareCleaned({text1},{text2}) AS value",
                map("text1",null,"text2",string2),
                row -> assertEquals(false, row.get("value")));
    }

    @Test
    public void testCompareCleanedInQuery() throws Exception {
        testCall(db,
                        "WITH apoc.text.clean({a}) as clean_a, " +
                        "apoc.text.clean({b}) as clean_b " +
                        "RETURN clean_a = clean_b as eq",
                map("a","&N[]eo 4 #J-(3.0)  ","b"," [N]e o4/J-[]3-0"),
                row -> assertEquals(true, row.get("eq")));
    }


    // Documentation tests
    // These are here to verify the claims made in string.adoc

    @Test
    public void testDocReplace() throws Exception {
        testCall(db,
                "RETURN apoc.text.regreplace('Hello World!', '[^a-zA-Z]', '')  AS value",
                row -> assertEquals("HelloWorld", row.get("value")));
    }

    @Test
    public void testDocJoin() throws Exception {
        testCall(db,
                "RETURN apoc.text.join(['Hello', 'World'], ' ') AS value",
                row -> assertEquals("Hello World", row.get("value")));
    }

    @Test
    public void testDocClean() throws Exception {
        testCall(db,
                "RETURN apoc.text.clean({text}) AS value",
                map("text","Hello World!"),
                row -> assertEquals("helloworld", row.get("value")));
    }

    @Test
    public void testDocCompareCleaned() throws Exception {
        testCall(db,
                "RETURN apoc.text.compareCleaned({text1}, {text2}) AS value",
                map("text1","Hello World!","text2","_hello-world_"),
                row -> assertEquals(true, row.get("value")));
    }

    @Test
    public void testUrlEncode() {
        testCall(db,
                "RETURN apoc.text.urlencode('ab cd=gh&ij?') AS value",
                row -> assertEquals("ab+cd%3Dgh%26ij%3F", row.get("value"))
        );
    }

    @Test
    public void testUrlDecode() {
        testCall(db,
                "RETURN apoc.text.urldecode('ab+cd%3Dgh%26ij%3F') AS value",
                row -> assertEquals("ab cd=gh&ij?", row.get("value"))
        );
    }

    @Test
    public void testUrlDecodeFailure() {
        thrown.expect(QueryExecutionException.class);
        thrown.expectMessage("Failed to invoke function `apoc.text.urldecode`: Caused by: java.lang.IllegalArgumentException: URLDecoder: Incomplete trailing escape (%) pattern");
        testCall(db,
                "RETURN apoc.text.urldecode('ab+cd%3Dgh%26ij%3')  AS value",
                row -> assertEquals("ab cd=gh&ij?", row.get("value"))
        );
    }


    @Test
    public void testLPad() {
        testCall(db, "RETURN apoc.text.lpad('ab',4,' ')    AS value", row -> assertEquals("  ab", row.get("value")));
        testCall(db, "RETURN apoc.text.lpad('ab',4,'0')    AS value", row -> assertEquals("00ab", row.get("value")));
        testCall(db, "RETURN apoc.text.lpad('ab',2,' ')    AS value", row -> assertEquals("ab", row.get("value")));
        testCall(db, "RETURN apoc.text.lpad('abcde',4,' ') AS value", row -> assertEquals("abcde", row.get("value")));
    }
    @Test
    public void testRPad() {
        testCall(db, "RETURN apoc.text.rpad('ab',4,' ')    AS value", row -> assertEquals("ab  ", row.get("value")));
        testCall(db, "RETURN apoc.text.rpad('ab',4,'0')    AS value", row -> assertEquals("ab00", row.get("value")));
        testCall(db, "RETURN apoc.text.rpad('ab',2,' ')    AS value", row -> assertEquals("ab", row.get("value")));
        testCall(db, "RETURN apoc.text.rpad('abcde',4,' ') AS value", row -> assertEquals("abcde", row.get("value")));
    }
    @Test
    public void testFormat() {
        testCall(db, "RETURN apoc.text.format(null,null) AS value", row -> assertEquals(null, row.get("value")));
        testCall(db, "RETURN apoc.text.format('ab',null) AS value", row -> assertEquals("ab", row.get("value")));
        testCall(db, "RETURN apoc.text.format('ab%s %d %.1f %s%n',['cd',42,3.14,true]) AS value", row -> assertEquals("abcd 42 3.1 true\n", row.get("value")));
    }

    @Test
    public void testRegexGroups() {
        testResult(db, "RETURN apoc.text.regexGroups('abc <link xxx1>yyy1</link> def <link xxx2>yyy2</link>','<link (\\\\w+)>(\\\\w+)</link>') AS result",
                result -> {
                    final List<Object> r = Iterators.single(result.columnAs("result"));

                    List<List<String>> expected = new ArrayList<>(Arrays.asList(
                            new ArrayList<String>(Arrays.asList("<link xxx1>yyy1</link>", "xxx1", "yyy1")),
                            new ArrayList<String>(Arrays.asList("<link xxx2>yyy2</link>", "xxx2", "yyy2"))
                    ));
                    assertTrue(r.containsAll(expected));
                });
    }

    @Test
    public void testRegexGroupsForNPE() {
        // throws no exception
        testCall(db, "RETURN apoc.text.regexGroups(null,'<link (\\\\w+)>(\\\\w+)</link>') AS result", row -> { });
        testCall(db, "RETURN apoc.text.regexGroups('abc',null) AS result", row -> { });
    }

}
