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
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testResult;
import static java.lang.Math.toIntExact;
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
    public void testSplit() throws Exception {
        String text = "1,  2, 3,4";
        String regex = ", *";

        testCall(db,
                "RETURN apoc.text.split({text}, {regex}) AS value",
                map("text", text, "regex", regex),
                row -> assertEquals(Arrays.asList("1", "2", "3", "4"), row.get("value")));

        testCall(db,
                "RETURN apoc.text.split({text}, {regex}, 2) AS value",
                map("text", text, "regex", regex),
                row -> assertEquals(Arrays.asList("1", "2, 3,4"), row.get("value")));
    }

    @Test
    public void testSplitWithNull() throws Exception {
        String text = "Hello World";
        String regex = " ";

        testCall(db,
                "RETURN apoc.text.split({text}, {regex}) AS value",
                map("text", null, "regex", regex),
                row -> assertEquals(null, row.get("value")));

        testCall(db,
                "RETURN apoc.text.split({text}, {regex}) AS value",
                map("text", text, "regex", null),
                row -> assertEquals(null, row.get("value")));

        testCall(db,
                "RETURN apoc.text.split({text}, {regex}, null) AS value",
                map("text", text, "regex", regex),
                row -> assertEquals(null, row.get("value")));

        testCall(db,
                "RETURN apoc.text.split({text}, {regex}) AS value",
                map("text", "", "regex", ""),
                row -> assertEquals(Collections.singletonList(""), row.get("value")));
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

    @Test
    public void testGetLevenshteinDistance() {
        String text1 = "Levenshtein";
        String text2 = "Levenstein";

        testCall(db, "RETURN apoc.text.distance({a}, {b}) as distance",
            map("a", text1, "b", text2),
            row -> assertEquals(1L, row.get("distance")));
    }

    @Test
    public void testFuzzyMatch() {
        Strings strings = new Strings();
        assertFalse(strings.fuzzyMatch("Th", "th"));
        assertFalse(strings.fuzzyMatch("THe", "the"));
        assertFalse(strings.fuzzyMatch("THEthe", "thethe"));
        assertTrue(strings.fuzzyMatch("The", "the"));
        assertTrue(strings.fuzzyMatch("THethe", "thethe"));
    }

    @Test
    public void testFuzzyMatchIntegration() {
        testCall(db, "RETURN apoc.text.fuzzyMatch({a}, {b}) as distance",
            map("a", "The", "b", "the"),
            row -> assertEquals(true, row.get("distance")));
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
    
    @Test
    public void testSlug() {
        testCall(db, "RETURN apoc.text.slug('a-b','-') AS value", row -> assertEquals("a-b", row.get("value")));
        testCall(db, "RETURN apoc.text.slug('a b  c') AS value", row -> assertEquals("a-b-c", row.get("value")));
        testCall(db, "RETURN apoc.text.slug(' a b-- c', '.') AS value", row -> assertEquals("a.b.c", row.get("value")));
        testCall(db, "RETURN apoc.text.slug(' a- b c ', '-') AS value", row -> assertEquals("a-b-c", row.get("value")));
        testCall(db, "RETURN apoc.text.slug('a- b c', '.') AS value", row -> assertEquals("a.b.c", row.get("value")));
        testCall(db, "RETURN apoc.text.slug('a- b','-') AS value", row -> assertEquals("a-b", row.get("value")));
        testCall(db, "RETURN apoc.text.slug('a b c') AS value", row -> assertEquals("a-b-c", row.get("value")));
    }

    @Test
    public void testRandom() {
        Long length = 10L;
        String valid = "A-Z0-9.";

        testCall(
                db,
                "RETURN apoc.text.random({length}, {valid}) as value",
                map("length", length, "valid", valid),
                row -> {
                    String value = row.get("value").toString();

                    Pattern pattern = Pattern.compile("^"+valid+"$");
                    Matcher matcher = pattern.matcher(value);

                    assertEquals(toIntExact(length), value.length());
                    assertTrue(matcher.matches());
                }
        );
    }

    @Test
    public void testCapitalize() {
        String text = "neo4j";

        testCall(
                db,
                "RETURN apoc.text.capitalize({text}) as value",
                map("text", text),
                row -> assertEquals("Neo4j", row.get("value").toString())
        );
    }


    @Test
    public void testCapitalizeAll() {
        String text = "graph database";

        testCall(
                db,
                "RETURN apoc.text.capitalizeAll({text}) as value",
                map("text", text),
                row -> assertEquals("Graph Database", row.get("value").toString())

        );
    }

    @Test
    public void testDecapitalize() {
        String text = "Graph Database";

        testCall(
                db,
                "RETURN apoc.text.capitalizeAll({text}) as value",
                map("text", text),
                row -> assertEquals("graph database", row.get("value").toString())

        );
    }

    @Test
    public void testSwapCase() {
        String text = "Neo4j";

        testCall(
                db,
                "RETURN apoc.text.swapCase({text}) as value",
                map("text", text),
                row -> assertEquals("nEO4J", row.get("value").toString())

        );
    }

    @Test
    public void testCamelCase() {
        String text = "test Camel case";

        testCall(
                db,
                "RETURN apoc.text.swapCase({text}) as value",
                map("text", text),
                row -> assertEquals("testCamelCase", row.get("value").toString())

        );
    }


    @Test
    public void testSnakeCase() {
        String text = "test snake CASE";

        testCall(
                db,
                "RETURN apoc.text.snakeCase({text}) as value",
                map("text", text),
                row -> assertEquals("test-snake-case", row.get("value").toString())

        );
    }

    @Test
    public void testToUpperCase() {
        String text = "test upper CASE";

        testCall(
                db,
                "RETURN apoc.text.toUpperCase({text}) as value",
                map("text", text),
                row -> assertEquals("TEST_UPPER_CASE", row.get("value").toString())

        );
    }
}
