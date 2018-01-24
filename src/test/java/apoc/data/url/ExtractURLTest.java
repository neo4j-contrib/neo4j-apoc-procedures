package apoc.data.url;

import apoc.util.TestUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.test.TestGraphDatabaseFactory;
import java.util.HashMap;

import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testCall;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;

public class ExtractURLTest {
    private GraphDatabaseService db;
    private HashMap<String,Object[]> testCases;
    String [] methods = new String [] { 
        "protocol", 
        "userInfo", 
        "host", 
        "port", 
        "path", 
        "file", 
        // "query", 
        // "anchor" 
    };
    

    @Before
    public void setUp() throws Exception {
        db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        TestUtil.registerProcedure(db, ExtractURL.class);

        // Test cases map URLs to an array of strings representing what their correct answers should
        // be for components:
        // protocol, user, host, port, path, file, query, anchor.
        // Ordering must match "methods" above.
        testCases = new HashMap<String,Object[]>();

        testCases.put("http://simple.com/", new Object [] {
            "http", null, "simple.com", -1L, "/", "/", null, null
        });

        // Something kinda complicated...
        testCases.put("https://user:secret@localhost:666/path/to/file.html?x=1#b", new Object [] {
            "https", "user:secret", "localhost", 666L, "/path/to/file.html", "/path/to/file.html?x=1", "x=1", "b"
        });

        // This is a malformed URL
        testCases.put("google.com", new Object [] {
            null, null, null, -1L, null, null, null, null
        });

        // Real-world semi complex URL.  So meta! :)
        testCases.put("https://github.com/neo4j-contrib/neo4j-apoc-procedures/commit/31cf4b60236ef5e08f7b231ed03f3d8ace511fd2#diff-a084b794bc0759e7a6b77810e01874f2",
            new Object [] {
                "https", null, "github.com", -1L,
                "/neo4j-contrib/neo4j-apoc-procedures/commit/31cf4b60236ef5e08f7b231ed03f3d8ace511fd2",
                "/neo4j-contrib/neo4j-apoc-procedures/commit/31cf4b60236ef5e08f7b231ed03f3d8ace511fd2",
                "diff-a084b794bc0759e7a6b77810e01874f2"
            });
    }

    @After
    public void tearDown() {
        db.shutdown();
    }

    @Test
    public void testByCase() {
        for(String url : testCases.keySet()) {
            Object [] answers = testCases.get(url);

            for(int x=0; x<methods.length; x++) {
                String method = methods[x];
                final int answerIdx = x;
                testCall(db, "RETURN apoc.data.url."+method+"('" + url + "') AS value",
                row -> assertEquals(answers[answerIdx], row.get("value")));
            }   
        }

        // Test all null cases.
        for (int x=0; x<methods.length; x++) {
            String method = methods[x];
            final int answerIdx = x;
            testCall(db, "RETURN apoc.data.url."+method+"(null) AS value",
                row -> {
                   if (method == "port") {
                       assertEquals(-1L, row.get("value"));
                       return;
                   } 
                   assertEquals(null, row.get("value"));
                   return;
                });
        }
    }

    /* Previous test cases matching the deprecated Extract class' requirements are below */

    @Test
    public void testNull() {
        testCall(db, "RETURN apoc.data.url.host(null) AS value",
                row -> assertEquals(null, row.get("value")));
    }

    @Test
    public void testBadString() {
        testCall(db, "RETURN apoc.data.url.host('asdsgawe4ge') AS value",
                row -> assertEquals(null, row.get("value")));
    }

    @Test
    public void testEmptyString() {
        testCall(db, "RETURN apoc.data.url.host('') AS value",
                row -> assertEquals(null, row.get("value")));
    }

    @Test
    public void testUrl() {
        testCall(db, "RETURN apoc.data.url.host('http://www.example.com/lots-of-stuff') AS value",
                row -> assertEquals("www.example.com", row.get("value")));
    }

    @Test
    public void testQueryParameter() {
        testCall(db, "RETURN apoc.data.url.host({param}) AS value",
                map("param", "http://www.foo.bar/baz"),
                row -> assertEquals("www.foo.bar", row.get("value")));
    }

    @Test
    public void testShorthandURL() {
        testCall(db, "RETURN apoc.data.url.host({param}) AS value",
            map("param", "partial.com/foobar"),
            row -> assertEquals(null, row.get("value")));
    }
}
