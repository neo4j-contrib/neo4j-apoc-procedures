package apoc.data.url;

import apoc.util.TestUtil;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.HashMap;
import java.util.Map;

import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testCall;
import static org.junit.Assert.assertEquals;

public class ExtractURLTest {

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    private static HashMap<String,Map<String,Object>> testCases;

    @BeforeClass
    public static void setUp() throws Exception {
        TestUtil.registerProcedure(db, ExtractURL.class);

        // Test cases map URLs to an array of strings representing what their correct answers should
        // be for components:
        // protocol, user, host, port, path, file, query, anchor.
        // Ordering must match "methods" above.
        testCases = new HashMap<>();

        testCases.put("http://simple.com/", map(
            "protocol","http", "user",null, "host","simple.com", "port",null, "path","/", "file","/", "query",null, "anchor",null)
        );

        // Something kinda complicated...
        testCases.put("https://user:secret@localhost:666/path/to/file.html?x=1#b", map(
            "protocol","https", "user","user:secret", "host","localhost", "port",666L, "path","/path/to/file.html", "file","/path/to/file.html?x=1","query", "x=1", "anchor","b"));

        // This is a malformed URL
        testCases.put("google.com", null);

        // Real-world semi complex URL.  So meta! :)
        testCases.put("https://github.com/neo4j-contrib/neo4j-apoc-procedures/commit/31cf4b60236ef5e08f7b231ed03f3d8ace511fd2#diff-a084b794bc0759e7a6b77810e01874f2",
            map ("protocol", "https", "user",null, "host","github.com", "port",null,
                "path","/neo4j-contrib/neo4j-apoc-procedures/commit/31cf4b60236ef5e08f7b231ed03f3d8ace511fd2",
                "file","/neo4j-contrib/neo4j-apoc-procedures/commit/31cf4b60236ef5e08f7b231ed03f3d8ace511fd2",
                "anchor","diff-a084b794bc0759e7a6b77810e01874f2", "query", null
            ));

        // non standard protocol
        testCases.put("neo4j://graphapps/neo4j-browser?cmd=play&arg=cypher", map(
                "protocol","neo4j", "user", null, "host","graphapps", "port",null, "path","/neo4j-browser", "file","/neo4j-browser?cmd=play&arg=cypher", "query","cmd=play&arg=cypher", "anchor",null)
        );

    }

    @Test
    public void testByCase() {
        for(String url : testCases.keySet()) {
            Map<String,Object> answers = testCases.get(url);

            testCall(db, "RETURN apoc.data.url($url) AS value",map("url",url),
            row -> {
                Map value = (Map) row.get("value");
                if (answers != null) {
                    for (Map.Entry o : answers.entrySet()) {
                        assertEquals(o.getKey().toString(), o.getValue(), value.get(o.getKey()));
                    }
                }
                assertEquals(answers, value);
            });
        }

        testCall(db, "RETURN apoc.data.url(null) AS value",
            row -> {
               assertEquals(null, row.get("value"));
            });
    }

    /* Previous test cases matching the deprecated Extract class' requirements are below */

    @Test
    public void testNull() {
        testCall(db, "RETURN apoc.data.url(null).host AS value",
                row -> assertEquals(null, row.get("value")));
    }

    @Test
    public void testBadString() {
        testCall(db, "RETURN apoc.data.url('asdsgawe4ge').host AS value",
                row -> assertEquals(null, row.get("value")));
    }

    @Test
    public void testEmptyString() {
        testCall(db, "RETURN apoc.data.url('').host AS value",
                row -> assertEquals(null, row.get("value")));
    }

    @Test
    public void testUrl() {
        testCall(db, "RETURN apoc.data.url('http://www.example.com/lots-of-stuff').host AS value",
                row -> assertEquals("www.example.com", row.get("value")));
    }

    @Test
    public void testQueryParameter() {
        testCall(db, "RETURN apoc.data.url($param).host AS value",
                map("param", "http://www.foo.bar/baz"),
                row -> assertEquals("www.foo.bar", row.get("value")));
    }

    @Test
    public void testShorthandURL() {
        testCall(db, "RETURN apoc.data.url($param).host AS value",
            map("param", "partial.com/foobar"),
            row -> assertEquals(null, row.get("value")));
    }
}
