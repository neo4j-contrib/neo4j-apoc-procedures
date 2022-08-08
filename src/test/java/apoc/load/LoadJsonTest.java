package apoc.load;

import apoc.ApocConfiguration;
import apoc.util.TestUtil;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.mockserver.client.server.MockServerClient;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.model.Header;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.Arrays;

import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testResult;
import static java.util.Arrays.asList;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockserver.integration.ClientAndServer.startClientAndServer;
import static org.mockserver.matchers.Times.exactly;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

public class LoadJsonTest {

    private static ClientAndServer mockServer;

    @BeforeClass
    public static void startServer() {
        mockServer = startClientAndServer(1080);
    }

    @AfterClass
    public static void stopServer() {
        mockServer.stop();
    }



    private GraphDatabaseService db;
	@Before public void setUp() throws Exception {
        URL url = ClassLoader.getSystemResource("map.json");
        db = new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder()
                .setConfig("apoc.import.file.enabled","true")
                .setConfig("apoc.import.file.use_neo4j_config", "false")
                .setConfig("apoc.json.zip.url","https://github.com/neo4j-contrib/neo4j-apoc-procedures/blob/3.4/src/test/resources/testload.zip?raw=true!person.json")
                .setConfig("apoc.json.simpleJson.url", url.toString())
                .setConfig(ApocConfiguration.CYPHER_IP_BLOCKLIST, "127.168.0.0/8")
                .newGraphDatabase();
        TestUtil.registerProcedure(db, LoadJson.class);
    }

    @After public void tearDown() {
	    db.shutdown();
    }

    @Test public void testLoadJson() throws Exception {
		URL url = ClassLoader.getSystemResource("map.json");
		testCall(db, "CALL apoc.load.json({url})",map("url",url.toString()), // 'file:map.json' YIELD value RETURN value
                (row) -> {
                    assertEquals(map("foo",asList(1L,2L,3L)), row.get("value"));
                });
    }

    @Test public void testLoadJsonFromBlockedIpRange() throws Exception {
        List<String> protocols = Arrays.asList("https", "http");

        for (String protocol: protocols) {
            QueryExecutionException e = Assert.assertThrows(QueryExecutionException.class,
                                                             () -> testCall(db,
                                                                           "CALL apoc.load.json('" + protocol + "://127.168.0.0/test.csv')",
                                                                           map(),
                                                                           (r) -> {}
                                                            )
            );
            assertTrue(e.getMessage().contains("access to /127.168.0.0 is blocked via the configuration property unsupported.dbms.cypher_ip_blocklist"));
        }
    }

    @Test public void testLoadMultiJson() throws Exception {
		URL url = ClassLoader.getSystemResource("multi.json");
		testResult(db, "CALL apoc.load.json({url})",map("url",url.toString()), // 'file:map.json' YIELD value RETURN value
                (result) -> {
                    Map<String, Object> row = result.next();
                    assertEquals(map("foo",asList(1L,2L,3L)), row.get("value"));
                    row = result.next();
                    assertEquals(map("bar",asList(4L,5L,6L)), row.get("value"));
                    assertFalse(result.hasNext());
                });
    }
    @Test public void testLoadMultiJsonPaths() throws Exception {
		URL url = ClassLoader.getSystemResource("multi.json");
		testResult(db, "CALL apoc.load.json({url},'$')",map("url",url.toString()), // 'file:map.json' YIELD value RETURN value
                (result) -> {
                    Map<String, Object> row = result.next();
                    assertEquals(map("foo",asList(1L,2L,3L)), row.get("value"));
                    row = result.next();
                    assertEquals(map("bar",asList(4L,5L,6L)), row.get("value"));
                    assertFalse(result.hasNext());
                });
    }
    @Test public void testLoadJsonPath() throws Exception {
		URL url = ClassLoader.getSystemResource("map.json");
		testCall(db, "CALL apoc.load.json({url},'$.foo')",map("url",url.toString()), // 'file:map.json' YIELD value RETURN value
                (row) -> {
                    assertEquals(map("result",asList(1L,2L,3L)), row.get("value"));
                });
    }
    @Test public void testLoadJsonPathRoot() throws Exception {
		URL url = ClassLoader.getSystemResource("map.json");
		testCall(db, "CALL apoc.load.json({url},'$')",map("url",url.toString()), // 'file:map.json' YIELD value RETURN value
                (row) -> {
                    assertEquals(map("foo",asList(1L,2L,3L)), row.get("value"));
                });
    }
    @Test public void testLoadJsonArrayPath() throws Exception {
		URL url = ClassLoader.getSystemResource("map.json");
		testCall(db, "CALL apoc.load.jsonArray({url},'$.foo')",map("url",url.toString()), // 'file:map.json' YIELD value RETURN value
                (row) -> {
                    assertEquals(asList(1L,2L,3L), row.get("value"));
                });
    }
    @Test public void testLoadJsonArrayPathRoot() throws Exception {
		URL url = ClassLoader.getSystemResource("map.json");
		testCall(db, "CALL apoc.load.jsonArray({url},'$')",map("url",url.toString()), // 'file:map.json' YIELD value RETURN value
                (row) -> {
                    assertEquals(map("foo",asList(1L,2L,3L)), row.get("value"));
                });
    }
    @Test @Ignore public void testLoadJsonGraphCommons() throws Exception {
		String url = "https://graphcommons.com/graphs/8da5327d-7829-4dfe-b60b-4c0bda956b2a.json";
		testCall(db, "CALL apoc.load.json({url})",map("url", url), // 'file:map.json' YIELD value RETURN value
                (row) -> {
                    Map value = (Map)row.get("value");
                    assertEquals(true, value.containsKey("users"));
                    assertEquals(true, value.containsKey("nodes"));
                });
    }

    @Test public void testLoadJsonStackOverflow() throws Exception {
        String url = "https://api.stackexchange.com/2.2/questions?pagesize=10&order=desc&sort=creation&tagged=neo4j&site=stackoverflow&filter=!5-i6Zw8Y)4W7vpy91PMYsKM-k9yzEsSC1_Uxlf";
        testCall(db, "CALL apoc.load.json({url})",map("url", url), // 'file:map.json' YIELD value RETURN value
                (row) -> {
                    Map<String, Object> value = (Map<String, Object>) row.get("value");
                    assertFalse(value.isEmpty());
                    List<Map<String, Object>> items = (List<Map<String, Object>>) value.get("items");
                    assertEquals(10, items.size());
                });
    }


    @Test public void testLoadJsonNoFailOnError() throws Exception {
        String url = "file.json";
        testResult(db, "CALL apoc.load.json({url},null, {failOnError:false})",map("url", url), // 'file:map.json' YIELD value RETURN value
                (row) -> {
                    assertFalse(row.hasNext());
                });
    }

    @Test public void testLoadJsonZip() throws Exception {
        URL url = ClassLoader.getSystemResource("testload.zip");
        testCall(db, "CALL apoc.load.json({url})",map("url",url.toString()+"!person.json"),
                (row) -> {
                    Map<String,Object> r = (Map<String, Object>) row.get("value");
                    assertEquals("Michael", r.get("name"));
                    assertEquals(41L, r.get("age"));
                    assertEquals(asList("Selina", "Rana", "Selma"), r.get("children"));
                });
    }

    @Test public void testLoadJsonTar() throws Exception {
        URL url = ClassLoader.getSystemResource("testload.tar");
        testCall(db, "CALL apoc.load.json({url})",map("url",url.toString()+"!person.json"),
                (row) -> {
                    Map<String,Object> r = (Map<String, Object>) row.get("value");
                    assertEquals("Michael", r.get("name"));
                    assertEquals(41L, r.get("age"));
                    assertEquals(asList("Selina", "Rana", "Selma"), r.get("children"));
                });
    }

    @Test public void testLoadJsonTarGz() throws Exception {
        URL url = ClassLoader.getSystemResource("testload.tar.gz");
        testCall(db, "CALL apoc.load.json({url})",map("url",url.toString()+"!person.json"),
                (row) -> {
                    Map<String,Object> r = (Map<String, Object>) row.get("value");
                    assertEquals("Michael", r.get("name"));
                    assertEquals(41L, r.get("age"));
                    assertEquals(asList("Selina", "Rana", "Selma"), r.get("children"));
                });
    }

    @Test public void testLoadJsonTgz() throws Exception {
        URL url = ClassLoader.getSystemResource("testload.tgz");
        testCall(db, "CALL apoc.load.json({url})",map("url",url.toString()+"!person.json"),
                (row) -> {
                    Map<String,Object> r = (Map<String, Object>) row.get("value");
                    assertEquals("Michael", r.get("name"));
                    assertEquals(41L, r.get("age"));
                    assertEquals(asList("Selina", "Rana", "Selma"), r.get("children"));
                });
    }

    @Test public void testLoadJsonZipByUrl() throws Exception {
        URL url = new URL("https://github.com/neo4j-contrib/neo4j-apoc-procedures/blob/3.4/src/test/resources/testload.zip?raw=true");
        testCall(db, "CALL apoc.load.json({url})",map("url",url.toString()+"!person.json"),
                (row) -> {
                    Map<String,Object> r = (Map<String, Object>) row.get("value");
                    assertEquals("Michael", r.get("name"));
                    assertEquals(41L, r.get("age"));
                    assertEquals(asList("Selina", "Rana", "Selma"), r.get("children"));
                });
    }

    @Test public void testLoadJsonTarByUrl() throws Exception {
        URL url = new URL("https://github.com/neo4j-contrib/neo4j-apoc-procedures/blob/3.4/src/test/resources/testload.tar?raw=true");
        testCall(db, "CALL apoc.load.json({url})",map("url",url.toString()+"!person.json"),
                (row) -> {
                    Map<String,Object> r = (Map<String, Object>) row.get("value");
                    assertEquals("Michael", r.get("name"));
                    assertEquals(41L, r.get("age"));
                    assertEquals(asList("Selina", "Rana", "Selma"), r.get("children"));
                });
    }

    @Test public void testLoadJsonTarGzByUrl() throws Exception {
        URL url = new URL("https://github.com/neo4j-contrib/neo4j-apoc-procedures/blob/3.4/src/test/resources/testload.tar.gz?raw=true");
        testCall(db, "CALL apoc.load.json({url})",map("url",url.toString()+"!person.json"),
                (row) -> {
                    Map<String,Object> r = (Map<String, Object>) row.get("value");
                    assertEquals("Michael", r.get("name"));
                    assertEquals(41L, r.get("age"));
                    assertEquals(asList("Selina", "Rana", "Selma"), r.get("children"));
                });
    }

    @Test public void testLoadJsonTgzByUrl() throws Exception {
        URL url = new URL("https://github.com/neo4j-contrib/neo4j-apoc-procedures/blob/3.4/src/test/resources/testload.tgz?raw=true");
        testCall(db, "CALL apoc.load.json({url})",map("url",url.toString()+"!person.json"),
                (row) -> {
                    Map<String,Object> r = (Map<String, Object>) row.get("value");
                    assertEquals("Michael", r.get("name"));
                    assertEquals(41L, r.get("age"));
                    assertEquals(asList("Selina", "Rana", "Selma"), r.get("children"));
                });
    }

    @Test public void testLoadJsonZipByUrlInConfigFile() throws Exception {
        testCall(db, "CALL apoc.load.json({key})",map("key","zip"),
                (row) -> {
                    Map<String,Object> r = (Map<String, Object>) row.get("value");
                    assertEquals("Michael", r.get("name"));
                    assertEquals(41L, r.get("age"));
                    assertEquals(asList("Selina", "Rana", "Selma"), r.get("children"));
                });
    }

    @Test public void testLoadJsonByUrlInConfigFile() throws Exception {

        testCall(db, "CALL apoc.load.json({key})",map("key","simpleJson"), // 'file:map.json' YIELD value RETURN value
                (row) -> {
                    assertEquals(map("foo",asList(1L,2L,3L)), row.get("value"));
                });
    }

    @Test(expected = QueryExecutionException.class)
    public void testLoadJsonByUrlInConfigFileWrongKey() throws Exception {

        try {
            testResult(db, "CALL apoc.load.json({key})",map("key","foo"), (r) -> {});
        } catch (QueryExecutionException e) {
            Throwable except = ExceptionUtils.getRootCause(e);
            assertTrue(except instanceof IOException);
            final String message = except.getMessage();
            System.out.println("This is the message: " + message);
            assertTrue(message.startsWith("no protocol: foo"));
            throw e;
        }
    }

    @Test
    public void testLoadJsonParams() throws Exception {
        new MockServerClient("localhost", 1080)
                .when(
                        request()
                                .withMethod("POST")
                                .withPath("/docs/search")
                                .withHeader("\"Content-type\", \"application/json\""),
                        exactly(1))
                .respond(
                        response()
                                .withStatusCode(200)
                                .withHeaders(
                                        new Header("Content-Type", "application/json; charset=utf-8"),
                                        new Header("Cache-Control", "public, max-age=86400"))
                                .withBody("{ result: 'message' }")
                                .withDelay(TimeUnit.SECONDS, 1)
                );


        testCall(db, "call apoc.load.jsonParams($url, $config, $json)",
                map("json", "{\"query\":\"pagecache\",\"version\":\"3.5\"}",
                        "url", "http://localhost:1080/docs/search",
                        "config", map("method", "POST")),
                (row) -> {
                    Map<String, Object> value = (Map<String, Object>) row.get("value");
                    assertFalse("value should be not empty", value.isEmpty());
                });
    }
}
