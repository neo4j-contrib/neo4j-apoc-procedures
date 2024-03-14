package apoc.load;

import apoc.util.CompressionAlgo;
import apoc.util.TestUtil;
import apoc.util.Util;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.junit.*;
import org.mockserver.client.MockServerClient;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.model.Header;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Result;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;
import org.testcontainers.containers.GenericContainer;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static apoc.ApocConfig.APOC_IMPORT_FILE_ENABLED;
import static apoc.ApocConfig.apocConfig;
import static apoc.util.BinaryTestUtil.fileToBinary;
import static apoc.util.CompressionConfig.COMPRESSION;
import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.*;
import static java.util.Arrays.asList;
import static org.junit.Assert.*;
import static org.mockserver.integration.ClientAndServer.startClientAndServer;
import static org.mockserver.matchers.Times.exactly;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

public class LoadCsvTest {

    private static ClientAndServer mockServer;

    private static final List<Map<String, Object>> RESPONSE_BODY = List.of(
            Map.of("headFoo", "one", "headBar", "two"),
            Map.of("headFoo", "three", "headBar", "four"),
            Map.of("headFoo", "five", "headBar", "six")
    );

    @BeforeClass
    public static void startServer() {
        mockServer = startClientAndServer(1080);
    }

    @AfterClass
    public static void stopServer() {
        mockServer.stop();
    }

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule()
                .withSetting(GraphDatabaseSettings.load_csv_file_url_root, Paths.get(getUrlFileName("test.csv").toURI()).getParent());

    private GenericContainer httpServer;

    public LoadCsvTest() throws URISyntaxException {
    }

    @Before public void setUp() throws Exception {
        TestUtil.registerProcedure(db, LoadCsv.class);
        apocConfig().setProperty(APOC_IMPORT_FILE_ENABLED, true);
    }

    @Test public void testLoadCsv() throws Exception {
        String url = "test.csv";
        testResult(db, "CALL apoc.load.csv($url,{results:['map','list','stringMap','strings']})", map("url",url), // 'file:test.csv'
                this::commonAssertionsLoadCsv);
    }

    @Test
    public void testLoadCsvWithBinary() {
        testResult(db, "CALL apoc.load.csvParams($file, null, null, $conf)", 
                map("file", fileToBinary(new File(getUrlFileName("test.csv").getPath()), CompressionAlgo.DEFLATE.name()),
                        "conf", map(COMPRESSION, CompressionAlgo.DEFLATE.name(), "results", List.of("map", "list", "stringMap", "strings"))),
                this::commonAssertionsLoadCsv);
    }

    private void commonAssertionsLoadCsv(Result r) {
        assertRow(r, 0L, "name", "Selma", "age", "8");
        assertRow(r, 1L, "name", "Rana", "age", "11");
        assertRow(r, 2L, "name", "Selina", "age", "18");
        assertFalse(r.hasNext());
    }

    /*
    WITH 'file:///test.csv' AS url
CALL apoc.load.csv(url,) YIELD map AS m
RETURN m.col_1,m.col_2,m.col_3
     */
    @Test public void testLoadCsvWithEmptyColumns() throws Exception {
        String url = "empty_columns.csv";
        testResult(db, "CALL apoc.load.csv($url,{failOnError:false,mapping:{col_2:{type:'int'}}})", map("url",url), // 'file:test.csv'
                (r) -> {
                    Map<String, Object> row = r.next();
                    assertEquals(map("col_1", "1","col_2", null,"col_3", "1"), row.get("map"));
                    row = r.next();
                    assertEquals(map("col_1", "2","col_2", 2L,"col_3", ""), row.get("map"));
                    row = r.next();
                    assertEquals(map("col_1", "3","col_2", 3L,"col_3", "3"), row.get("map"));
                    assertEquals(false, r.hasNext());
                });
        testResult(db, "CALL apoc.load.csv($url,{failOnError:false,nullValues:[''], mapping:{col_1:{type:'int'}}})", map("url",url), // 'file:test.csv'
                (r) -> {
                    Map<String, Object> row = r.next();
                    assertEquals(map("col_1", 1L,"col_2", null,"col_3", "1"), row.get("map"));
                    row = r.next();
                    assertEquals(map("col_1", 2L,"col_2", "2","col_3", null), row.get("map"));
                    row = r.next();
                    assertEquals(map("col_1", 3L,"col_2", "3","col_3", "3"), row.get("map"));
                    assertEquals(false, r.hasNext());
                });
        testResult(db, "CALL apoc.load.csv($url,{failOnError:false,mapping:{col_3:{type:'int',nullValues:['']}}})", map("url",url), // 'file:test.csv'
                (r) -> {
                    Map<String, Object> row = r.next();
                    assertEquals(map("col_1", "1","col_2", "","col_3", 1L), row.get("map"));
                    row = r.next();
                    assertEquals(map("col_1", "2","col_2", "2","col_3", null), row.get("map"));
                    row = r.next();
                    assertEquals(map("col_1", "3","col_2", "3","col_3", 3L), row.get("map"));
                    assertEquals(false, r.hasNext());
                });
    }

    static void assertRow(Result r, long lineNo, Object...data) {
        Map<String, Object> row = r.next();
        Map<String, Object> map = map(data);
        assertEquals(map, row.get("map"));
        Map<Object, Object> stringMap = new LinkedHashMap<>(map.size());
        map.forEach((k,v) -> stringMap.put(k,v == null ? null : v.toString()));
        assertEquals(stringMap, row.get("stringMap"));
        assertEquals(new ArrayList<>(map.values()), row.get("list"));
        assertEquals(new ArrayList<>(stringMap.values()), row.get("strings"));
        assertEquals(lineNo, row.get("lineNo"));
    }
    public static void assertRow(Result r, String name, String age, long lineNo) {
        Map<String, Object> row = r.next();
        assertEquals(map("name", name,"age", age), row.get("map"));
        assertEquals(asList(name, age), row.get("list"));
        assertEquals(lineNo, row.get("lineNo"));
    }

    @Test public void testLoadCsvSkipLimit() throws Exception {
        String url = "test.csv";
        testResult(db, "CALL apoc.load.csv($url,{skip:1,limit:1,results:['map','list','stringMap','strings']})", map("url",url), // 'file:test.csv'
                (r) -> {
                    assertRow(r, "Rana", "11", 1L);
                    assertEquals(false, r.hasNext());
                });
    }

    @Test public void testLoadCsvSkip() throws Exception {
        String url = "test.csv";
        testResult(db, "CALL apoc.load.csv($url,{skip:1,results:['map','list','stringMap','strings']})", map("url",url), // 'file:test.csv'
                (r) -> {
                    assertRow(r, "Rana", "11", 1L);
                    assertEquals(true, r.hasNext());
                    assertRow(r, "Selina", "18", 2L);
                    assertEquals(false, r.hasNext());
                });
    }

    @Test public void testLoadCsvTabSeparator() throws Exception {
        String url = "test-tab.csv";
        testResult(db, "CALL apoc.load.csv($url,{sep:'TAB',results:['map','list','stringMap','strings']})", map("url",url), // 'file:test.csv'
                (r) -> {
                    assertRow(r, 0L,"name", "Rana", "age","11");
                    assertEquals(false, r.hasNext());
                });
    }

    @Test 
    public void testLoadCsvEscape() { 
        URL url = getUrlFileName("test-escape.csv");
        final List<String> results = List.of("map", "list", "stringMap", "strings");
        testResult(db, "CALL apoc.load.csv($url, $config)",
                map("url", url.toString(), "config", map("results", results)),
                (r) -> {
                    assertRow(r, 0L,"name", "Naruto", "surname","Uzumaki");
                    assertRow(r, 1L,"name", "Minato", "surname","Namikaze");
                    assertFalse(r.hasNext());
                });
        testResult(db, "CALL apoc.load.csv($url,$config)",
                map("url", url.toString(), "config", map("results", results, "escapeChar", "NONE")),
                (r) -> {
                    assertRow(r, 0L,"name", "Narut\\o", "surname","Uzu\\maki");
                    assertRow(r, 1L,"name", "Minat\\o", "surname","Nami\\kaze");
                    assertFalse(r.hasNext());
                });
    }

    @Test
    public void testLoadCsvWithEscapedDelimiters() {
        String url = "test-escaped-delimiters.csv";
        final List<String> results = List.of("map", "list", "stringMap", "strings");

        /* In OpenCSV library 5.1 -> 5.2 they corrected a bug: https://sourceforge.net/p/opencsv/bugs/212/
           Now when we have an escaping character before a delimiter,
           the delimiter is ignored

           This means before a line:
                one\,two

           with the escaping character '\' and ',' as separator would parse
           as two columns. As in first it splits, then escapes.
           Now it looks like the new version of the library first escapes
           (so the ',' is escaped)
           and then splits, so it parses as one column.
        */
        var e = assertThrows(RuntimeException.class, () -> testResult(db, "CALL apoc.load.csv($url, $config)",
                       map("url", url, "config", map("results", results)),
                       (r) -> {
                           // Consume the stream so it throws the exception
                           r.stream().toArray();
                       })
            );
        assertTrue(e.getMessage().contains("Please check whether you included a delimiter before a column separator or forgot a column separator."));

        testResult(db, "CALL apoc.load.csv($url,$config)",
                   map("url", url, "config", map("results", results, "escapeChar", "NONE")),
                   (r) -> {
                       assertRow(r, 0L,"name", "Narut\\o\\", "surname","Uzu\\maki");
                       assertFalse(r.hasNext());
                   });
    }

    @Test public void testLoadCsvNoHeader() throws Exception {
        String url = "test-no-header.csv";
        testResult(db, "CALL apoc.load.csv($url,{header:false,results:['map','list','stringMap','strings']})", map("url",url), // 'file:test.csv'
                (r) -> {
                    Map<String, Object> row = r.next();
                    assertEquals(null, row.get("map"));
                    assertEquals(asList("Selma", "8"), row.get("list"));
                    assertEquals(0L, row.get("lineNo"));
                    assertEquals(false, r.hasNext());
                });
    }
    @Test public void testLoadCsvIgnoreFields() throws Exception {
        String url = "test-tab.csv";
        testResult(db, "CALL apoc.load.csv($url,{ignore:['age'],sep:'TAB',results:['map','list','stringMap','strings']})", map("url",url), // 'file:test.csv'
                (r) -> {
                    assertRow(r,0L,"name","Rana");
                    assertEquals(false, r.hasNext());
                });
    }

    @Test public void testLoadCsvColonSeparator() throws Exception {
        String url = "test.dsv";
        testResult(db, "CALL apoc.load.csv($url,{sep:':',results:['map','list','stringMap','strings']})", map("url",url), // 'file:test.csv'
                (r) -> {
                    assertRow(r,0L,"name","Rana","age","11");
                    assertFalse(r.hasNext());
                });
    }

    @Test public void testPipeArraySeparator() throws Exception {
        String url = "test-pipe-column.csv";
        testResult(db, "CALL apoc.load.csv($url,{results:['map','list','stringMap','strings'],mapping:{name:{type:'string'},beverage:{array:true,arraySep:'|',type:'string'}}})", map("url",url), // 'file:test.csv'
                (r) -> {
                    assertEquals(asList("Selma", asList("Soda")), r.next().get("list"));
                    assertEquals(asList("Rana", asList("Tea", "Milk")), r.next().get("list"));
                    assertEquals(asList("Selina", asList("Cola")), r.next().get("list"));
                });
    }

    @Test public void testWithSpacesInFileName() throws Exception {
        String url = "test pipe column with spaces in filename.csv";
        testResult(db, "CALL apoc.load.csv($url,{results:['map','list','stringMap','strings'],mapping:{name:{type:'string'},beverage:{array:true,arraySep:'|',type:'string'}}})", map("url",url), // 'file:test.csv'
                (r) -> {
                    assertEquals(asList("Selma", asList("Soda")), r.next().get("list"));
                    assertEquals(asList("Rana", asList("Tea", "Milk")), r.next().get("list"));
                    assertEquals(asList("Selina", asList("Cola")), r.next().get("list"));
                });
    }
    
    @Test
    public void testLoadCsvWithBom() {
        String url = "taxonomy.csv";
        testCall(db, "CALL apoc.load.csv($url) YIELD map return map['smth1'] as first, map['ceva'] as second",
                map("url",url),
                (row) -> {
                    final Object first = row.get("first");
                    final Object second = row.get("second");
                    assertEquals("Taxonomy", first);
                    assertEquals("1", second);
                });
    }

    @Test public void testMapping() throws Exception {
        String url = "test-mapping.csv";
        testResult(db, "CALL apoc.load.csv($url,{results:['map','list','stringMap','strings'],mapping:{name:{type:'string'},age:{type:'int'},kids:{array:true,arraySep:':',type:'int'},pass:{ignore:true}}})", map("url",url), // 'file:test.csv'
                (r) -> {
                    Map<String, Object> row = r.next();
                    assertEquals(map("name", "Michael", "age", 41L, "kids", asList(8L, 11L, 18L)), row.get("map"));
                    assertEquals(map("name", "Michael", "age", "41", "kids", "8:11:18"), row.get("stringMap"));
                    assertEquals(asList("Michael", 41L, asList(8L, 11L, 18L)), row.get("list"));
                    assertEquals(asList("Michael", "41", "8:11:18"), row.get("strings"));
                    assertEquals(0L, row.get("lineNo"));
                    assertEquals(false, r.hasNext());
                });
    }

    @Test
    public void testLoadCsvByUrl() throws Exception {
        URL url = new URL("https://raw.githubusercontent.com/neo4j-contrib/neo4j-apoc-procedures/3.1/src/test/resources/test.csv");
        testResult(db, "CALL apoc.load.csv($url,{results:['map','list','stringMap','strings']})", map("url", url.toString()),
                (r) -> {
                    assertRow(r,0L,"name","Selma","age","8");
                    assertRow(r,1L,"name","Rana","age","11");
                    assertRow(r,2L,"name","Selina","age","18");
                    assertEquals(false, r.hasNext());
                });

    }

    @Test
    public void testLoadCsvWithUserPassInUrl() throws JsonProcessingException {
        String userPass = "user:password";
        String token = Util.encodeUserColonPassToBase64(userPass);

        new MockServerClient("localhost", 1080)
                .when(
                        request()
                                .withPath("/docs/csv")
                                .withHeader("Authorization", "Basic " + token),
                        exactly(1))
                .respond(
                        response()
                                .withStatusCode(200)
                                .withHeaders(
                                        new Header("Content-Type", "text/csv; charset=utf-8"),
                                        new Header("Cache-Control", "private, max-age=1000"))
                                .withBody(fromListOfMapToCsvString(RESPONSE_BODY))
                                .withDelay(TimeUnit.SECONDS, 1)
                );

        testResult(db, "CALL apoc.load.csv($url, {results:['map']}) YIELD map",
                    map("url", "http://" + userPass + "@localhost:1080/docs/csv"),
                    (row) -> assertEquals(RESPONSE_BODY, row.stream().map(i->i.get("map")).collect(Collectors.toList()))
                );
    }

    @Test
    public void testLoadCsvParamsWithUserPassInUrl() throws JsonProcessingException {
        String userPass = "user:password";
        String token = Util.encodeUserColonPassToBase64(userPass);

        new MockServerClient("localhost", 1080)
                .when(
                        request()
                                .withMethod("POST")
                                .withPath("/docs/csv")
                                .withHeader("Authorization", "Basic " + token),
                        exactly(1))
                .respond(
                        response()
                                .withStatusCode(200)
                                .withHeaders(
                                        new Header("Content-Type", "text/csv; charset=utf-8"),
                                        new Header("Cache-Control", "private, max-age=100"))
                                .withBody(fromListOfMapToCsvString(RESPONSE_BODY))
                                .withDelay(TimeUnit.SECONDS, 1)
                );

        testResult(db, "CALL apoc.load.csvParams($url, $header, $payload, {results:['map','list','stringMap','strings']})",
                    map("url", "http://" + userPass + "@localhost:1080/docs/csv",
                        "header", map("method", "POST"),
                        "payload", "{\"query\":\"pagecache\",\"version\":\"3.5\"}"),
                    (row) -> assertEquals(RESPONSE_BODY, row.stream().map(i->i.get("map")).collect(Collectors.toList()))
                );
    }

    @Test
    public void testLoadCsvParamsWithBasicAuth() throws JsonProcessingException {
        String userPass = "user:password";
        String token = Util.encodeUserColonPassToBase64(userPass);

        new MockServerClient("localhost", 1080)
                .when(
                        request()
                                .withMethod("POST")
                                .withPath("/docs/csv")
                                .withHeader("Authorization", "Basic " + token)
                                .withHeader("Content-type", "application/json"),
                        exactly(1))
                .respond(
                        response()
                                .withStatusCode(200)
                                .withHeaders(
                                        new Header("Content-Type", "text/csv; charset=utf-8"),
                                        new Header("Cache-Control", "private, max-age=100"))
                                .withBody(fromListOfMapToCsvString(RESPONSE_BODY))
                                .withDelay(TimeUnit.SECONDS, 1)
                );

        testResult(db, "CALL apoc.load.csvParams($url, $header, $payload, {results:['map','list','stringMap','strings']})",
                    map("url", "http://localhost:1080/docs/csv",
                        "header", map("method",
                                    "POST", "Authorization", "Basic " + token,
                                    "Content-Type", "application/json"),
                            "payload", "{\"query\":\"pagecache\",\"version\":\"3.5\"}"),
                    (row) -> assertEquals(RESPONSE_BODY, row.stream().map(i->i.get("map")).collect(Collectors.toList()))
                );
    }

    @Test
    public void testLoadCsvByUrlRedirect() throws Exception {
        URL url = new URL("https://bit.ly/2nXgHA2");
        testResult(db, "CALL apoc.load.csv($url,{results:['map','list','stringMap','strings']})", map("url", url.toString()),
                (r) -> {
                    assertRow(r,0L,"name","Selma","age","8");
                    assertRow(r,1L,"name","Rana","age","11");
                    assertRow(r,2L,"name","Selina","age","18");
                    assertEquals(false, r.hasNext());
                });
    }

    @Test
    public void testLoadCsvNoFailOnError() throws Exception {
        String url = getUrlFileName("test.csv").getPath();
        testResult(db, "CALL apoc.load.csv($url,{failOnError:false})", map("url",url), // 'file:test.csv'
                (r) -> {
                    Map<String, Object> row = r.next();
                    assertEquals(0L, row.get("lineNo"));
                    assertEquals(asList("Selma","8"), row.get("list"));
                    assertEquals(Util.map("name","Selma","age","8"), row.get("map"));
                    assertEquals(true, r.hasNext());
                    row = r.next();
                    assertEquals(1L, row.get("lineNo"));
                    assertEquals(asList("Rana","11"), row.get("list"));
                    assertEquals(Util.map("name","Rana","age","11"), row.get("map"));
                    assertEquals(true, r.hasNext());
                    row = r.next();
                    assertEquals(2L, row.get("lineNo"));
                    assertEquals(asList("Selina","18"), row.get("list"));
                    assertEquals(Util.map("name","Selina","age","18"), row.get("map"));
                    assertEquals(false, r.hasNext());
                });
    }

    @Test public void testLoadCsvZip() throws Exception {
        String url = "testload.zip";
        testResult(db, "CALL apoc.load.csv($url,{results:['map','list','stringMap','strings']})", map("url",url+"!csv/test.csv"), // 'file:test.csv'
                (r) -> {
                    assertRow(r,0L,"name","Selma","age","8");
                    assertRow(r,1L,"name","Rana","age","11");
                    assertRow(r,2L,"name","Selina","age","18");
                    assertEquals(false, r.hasNext());
                });
    }

    @Test public void testLoadCsvTar() throws Exception {
        String url = "testload.tar";
        testResult(db, "CALL apoc.load.csv($url,{results:['map','list','stringMap','strings']})", map("url",url+"!csv/test.csv"), // 'file:test.csv'
                (r) -> {
                    assertRow(r,0L,"name","Selma","age","8");
                    assertRow(r,1L,"name","Rana","age","11");
                    assertRow(r,2L,"name","Selina","age","18");
                    assertEquals(false, r.hasNext());
                });
    }

    @Test public void testLoadCsvTarGz() throws Exception {
        String url = "testload.tar.gz";
        testResult(db, "CALL apoc.load.csv($url,{results:['map','list','stringMap','strings']})", map("url",url+"!csv/test.csv"), // 'file:test.csv'
                (r) -> {
                    assertRow(r,0L,"name","Selma","age","8");
                    assertRow(r,1L,"name","Rana","age","11");
                    assertRow(r,2L,"name","Selina","age","18");
                    assertEquals(false, r.hasNext());
                });
    }

    @Test public void testLoadCsvTgz() throws Exception {
        String url = "testload.tgz";
        testResult(db, "CALL apoc.load.csv($url,{results:['map','list','stringMap','strings']})", map("url",url+"!csv/test.csv"), // 'file:test.csv'
                (r) -> {
                    assertRow(r,0L,"name","Selma","age","8");
                    assertRow(r,1L,"name","Rana","age","11");
                    assertRow(r,2L,"name","Selina","age","18");
                    assertEquals(false, r.hasNext());
                });
    }

    @Test public void testLoadCsvZipByUrl() throws Exception {
        URL url = new URL("https://github.com/neo4j-contrib/neo4j-apoc-procedures/blob/3.4/src/test/resources/testload.zip?raw=true");
        testResult(db, "CALL apoc.load.csv($url,{results:['map','list','stringMap','strings']})", map("url",url.toString()+"!csv/test.csv"), // 'file:test.csv'
                (r) -> {
                    assertRow(r,0L,"name","Selma","age","8");
                    assertRow(r,1L,"name","Rana","age","11");
                    assertRow(r,2L,"name","Selina","age","18");
                    assertEquals(false, r.hasNext());
                });
    }

    @Test public void testLoadCsvTarByUrl() throws Exception {
        URL url = new URL("https://github.com/neo4j/apoc/blob/dev/core/src/test/resources/testload.tar?raw=true");
        testResult(db, "CALL apoc.load.csv($url,{results:['map','list','stringMap','strings']})", map("url",url.toString()+"!csv/test.csv"), // 'file:test.csv'
                (r) -> {
                    assertRow(r,0L,"name","Selma","age","8");
                    assertRow(r,1L,"name","Rana","age","11");
                    assertRow(r,2L,"name","Selina","age","18");
                    assertEquals(false, r.hasNext());
                });
    }

    @Test public void testLoadCsvTarGzByUrl() throws Exception {
        URL url = new URL("https://github.com/neo4j/apoc/blob/dev/core/src/test/resources/testload.tar.gz?raw=true");
        testResult(db, "CALL apoc.load.csv($url,{results:['map','list','stringMap','strings']})", map("url",url.toString()+"!csv/test.csv"), // 'file:test.csv'
                (r) -> {
                    assertRow(r,0L,"name","Selma","age","8");
                    assertRow(r,1L,"name","Rana","age","11");
                    assertRow(r,2L,"name","Selina","age","18");
                    assertEquals(false, r.hasNext());
                });
    }

    @Test public void testLoadCsvTgzByUrl() throws Exception {
        URL url = new URL("https://github.com/neo4j/apoc/blob/dev/core/src/test/resources/testload.tgz?raw=true");
        testResult(db, "CALL apoc.load.csv($url,{results:['map','list','stringMap','strings']})", map("url",url.toString()+"!csv/test.csv"), // 'file:test.csv'
                (r) -> {
                    assertRow(r,0L,"name","Selma","age","8");
                    assertRow(r,1L,"name","Rana","age","11");
                    assertRow(r,2L,"name","Selina","age","18");
                    assertEquals(false, r.hasNext());
                });
    }

    @Test(expected = QueryExecutionException.class)
    public void testLoadRedirectWithProtocolChange() {
        httpServer = new GenericContainer("alpine")
                .withCommand("/bin/sh", "-c", "while true; do { echo -e 'HTTP/1.1 301 Moved Permanently\\r\\nLocation: file:/etc/passwd'; echo ; } | nc -l -p 8000; done")
                .withExposedPorts(8000);
        httpServer.start();
        String url = String.format("http://%s:%s", httpServer.getContainerIpAddress(), httpServer.getMappedPort(8000));
        try {
            testResult(db, "CALL apoc.load.csv($url)", map("url", url),
                    (r) -> r.hasNext());
        } catch (QueryExecutionException e) {
            assertTrue(e.getMessage().contains("The redirect URI has a different protocol: file:/etc/passwd"));
            throw e;
        } finally {
            httpServer.stop();
        }
    }

    @Ignore("long running test")
    @Test public void testWithEmptyQuoteChar() throws Exception {
        //TODO: fix this test to not download 7 MB each time.
        Assume.assumeFalse("skip this in CI it downloads 7.3 MB of data", TestUtil.isRunningInCI());
        URL url = new URL("https://www.fhwa.dot.gov/bridge/nbi/2010/delimited/AL10.txt");
        testResult(db, "CALL apoc.load.csv($url, {quoteChar: '\0'})", map("url",url.toString()),
                (r) -> assertEquals(16018L, r.stream().count()));
    }

    private static String fromListOfMapToCsvString(List<Map<String, Object>> mapList ) throws JsonProcessingException {
        return new CsvMapper().writerFor(List.class)
                .with(CsvSchema.builder()
                        .addColumn("headFoo")
                        .addColumn("headBar")
                        .build()
                        .withHeader())
                .writeValueAsString(mapList);
    }
}
