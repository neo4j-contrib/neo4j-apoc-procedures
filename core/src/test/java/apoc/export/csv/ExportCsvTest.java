/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package apoc.export.csv;

import static apoc.util.BinaryTestUtil.getDecompressedData;
import static apoc.util.CompressionAlgo.DEFLATE;
import static apoc.util.CompressionAlgo.GZIP;
import static apoc.util.CompressionAlgo.NONE;
import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.assertError;
import static apoc.util.TestUtil.testResult;
import static apoc.util.Util.INVALID_QUERY_MODE_ERROR;
import static java.nio.charset.StandardCharsets.UTF_8;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import apoc.ApocSettings;
import apoc.graph.Graphs;
import apoc.meta.Meta;
import apoc.util.BinaryTestUtil;
import apoc.util.CompressionAlgo;
import apoc.util.CompressionConfig;
import apoc.util.TestUtil;
import apoc.util.Util;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Result;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

/**
 * @author mh
 * @since 22.05.16
 */
public class ExportCsvTest {

    private static final String EXPECTED_QUERY_NODES = String.format("\"u\"%n"
            + "\"{\"\"id\"\":0,\"\"labels\"\":[\"\"User\"\",\"\"User1\"\"],\"\"properties\"\":{\"\"name\"\":\"\"foo\"\",\"\"male\"\":true,\"\"age\"\":42,\"\"kids\"\":[\"\"a\"\",\"\"b\"\",\"\"c\"\"]}}\"%n"
            + "\"{\"\"id\"\":1,\"\"labels\"\":[\"\"User\"\"],\"\"properties\"\":{\"\"name\"\":\"\"bar\"\",\"\"age\"\":42}}\"%n"
            + "\"{\"\"id\"\":2,\"\"labels\"\":[\"\"User\"\"],\"\"properties\"\":{\"\"age\"\":12}}\"%n");
    private static final String EXPECTED_QUERY =
            String.format("\"u.age\",\"u.name\",\"u.male\",\"u.kids\",\"labels(u)\"%n"
                    + "\"42\",\"foo\",\"true\",\"[\"\"a\"\",\"\"b\"\",\"\"c\"\"]\",\"[\"\"User1\"\",\"\"User\"\"]\"%n"
                    + "\"42\",\"bar\",\"\",\"\",\"[\"\"User\"\"]\"%n"
                    + "\"12\",\"\",\"\",\"\",\"[\"\"User\"\"]\"%n");
    private static final String EXPECTED_QUERY_WITHOUT_QUOTES = String.format(
            "u.age,u.name,u.male,u.kids,labels(u)%n" + "42,foo,true,[\"a\",\"b\",\"c\"],[\"User1\",\"User\"]%n"
                    + "42,bar,,,[\"User\"]%n"
                    + "12,,,,[\"User\"]%n");
    private static final String EXPECTED_QUERY_QUOTES_NONE = String.format(
            "a.name,a.city,a.street,labels(a)%n" + "Andrea,Milano,Via Garibaldi, 7,[\"Address1\",\"Address\"]%n"
                    + "Bar Sport,,,[\"Address\"]%n"
                    + ",,via Benni,[\"Address\"]%n");
    private static final String EXPECTED_QUERY_QUOTES_ALWAYS =
            String.format("\"a.name\",\"a.city\",\"a.street\",\"labels(a)\"%n"
                    + "\"Andrea\",\"Milano\",\"Via Garibaldi, 7\",\"[\"\"Address1\"\",\"\"Address\"\"]\"%n"
                    + "\"Bar Sport\",\"\",\"\",\"[\"\"Address\"\"]\"%n"
                    + "\"\",\"\",\"via Benni\",\"[\"\"Address\"\"]\"%n");
    private static final String EXPECTED_QUERY_QUOTES_NEEDED = String.format("a.name,a.city,a.street,labels(a)%n"
            + "Andrea,Milano,\"Via Garibaldi, 7\",\"[\"\"Address1\"\",\"\"Address\"\"]\"%n"
            + "Bar Sport,,,\"[\"\"Address\"\"]\"%n"
            + ",,via Benni,\"[\"\"Address\"\"]\"%n");
    private static final String EXPECTED = String.format(
            "\"_id\",\"_labels\",\"age\",\"city\",\"kids\",\"male\",\"name\",\"street\",\"_start\",\"_end\",\"_type\"%n"
                    + "\"0\",\":User:User1\",\"42\",\"\",\"[\"\"a\"\",\"\"b\"\",\"\"c\"\"]\",\"true\",\"foo\",\"\",,,%n"
                    + "\"1\",\":User\",\"42\",\"\",\"\",\"\",\"bar\",\"\",,,%n"
                    + "\"2\",\":User\",\"12\",\"\",\"\",\"\",\"\",\"\",,,%n"
                    + "\"3\",\":Address:Address1\",\"\",\"Milano\",\"\",\"\",\"Andrea\",\"Via Garibaldi, 7\",,,%n"
                    + "\"4\",\":Address\",\"\",\"\",\"\",\"\",\"Bar Sport\",\"\",,,%n"
                    + "\"5\",\":Address\",\"\",\"\",\"\",\"\",\"\",\"via Benni\",,,%n"
                    + ",,,,,,,,\"0\",\"1\",\"KNOWS\"%n"
                    + ",,,,,,,,\"3\",\"4\",\"NEXT_DELIVERY\"%n");

    private static final String EXP_SAMPLE =
            "\"_id\",\"_labels\",\"address\",\"age\",\"baz\",\"city\",\"foo\",\"kids\",\"last:Name\",\"male\",\"name\",\"street\",\"_start\",\"_end\",\"_type\",\"one\",\"three\"\n"
                    + "\"0\",\":User:User1\",\"\",\"42\",\"\",\"\",\"\",\"[\"\"a\"\",\"\"b\"\",\"\"c\"\"]\",\"\",\"true\",\"foo\",\"\",,,,,\n"
                    + "\"1\",\":User\",\"\",\"42\",\"\",\"\",\"\",\"\",\"\",\"\",\"bar\",\"\",,,,,\n"
                    + "\"2\",\":User\",\"\",\"12\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",,,,,\n"
                    + "\"3\",\":Address:Address1\",\"\",\"\",\"\",\"Milano\",\"\",\"\",\"\",\"\",\"Andrea\",\"Via Garibaldi, 7\",,,,,\n"
                    + "\"4\",\":Address\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"Bar Sport\",\"\",,,,,\n"
                    + "\"5\",\":Address\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"via Benni\",,,,,\n"
                    + "\"6\",\":Sample:User\",\"\",\"\",\"\",\"\",\"\",\"\",\"Galilei\",\"\",\"\",\"\",,,,,\n"
                    + "\"7\",\":Sample:User\",\"Universe\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",,,,,\n"
                    + "\"8\",\":Sample:User\",\"\",\"\",\"\",\"\",\"bar\",\"\",\"\",\"\",\"\",\"\",,,,,\n"
                    + "\"9\",\":Sample:User\",\"\",\"\",\"baa\",\"\",\"true\",\"\",\"\",\"\",\"\",\"\",,,,,\n"
                    + ",,,,,,,,,,,,\"0\",\"1\",\"KNOWS\",\"\",\"\"\n"
                    + ",,,,,,,,,,,,\"3\",\"4\",\"NEXT_DELIVERY\",\"\",\"\"\n"
                    + ",,,,,,,,,,,,\"8\",\"9\",\"KNOWS\",\"two\",\"four\"\n";

    private static final String EXPECTED_NONE_QUOTES =
            String.format("_id,_labels,age,city,kids,male,name,street,_start,_end,_type%n"
                    + "0,:User:User1,42,,[\"a\",\"b\",\"c\"],true,foo,,,,%n"
                    + "1,:User,42,,,,bar,,,,%n"
                    + "2,:User,12,,,,,,,,%n"
                    + "3,:Address:Address1,,Milano,,,Andrea,Via Garibaldi, 7,,,%n"
                    + "4,:Address,,,,,Bar Sport,,,,%n"
                    + "5,:Address,,,,,,via Benni,,,%n"
                    + ",,,,,,,,0,1,KNOWS%n"
                    + ",,,,,,,,3,4,NEXT_DELIVERY%n");
    private static final String EXPECTED_NEEDED_QUOTES =
            String.format("_id,_labels,age,city,kids,male,name,street,_start,_end,_type%n"
                    + "0,:User:User1,42,,\"[\"\"a\"\",\"\"b\"\",\"\"c\"\"]\",true,foo,,,,%n"
                    + "1,:User,42,,,,bar,,,,%n"
                    + "2,:User,12,,,,,,,,%n"
                    + "3,:Address:Address1,,Milano,,,Andrea,\"Via Garibaldi, 7\",,,%n"
                    + "4,:Address,,,,,Bar Sport,,,,%n"
                    + "5,:Address,,,,,,via Benni,,,%n"
                    + ",,,,,,,,0,1,KNOWS%n"
                    + ",,,,,,,,3,4,NEXT_DELIVERY%n");
    private static final String EXPECTED_QUOTES_ALWAYS =
            "\"_id\",\"_labels\",\"age\",\"city\",\"kids\",\"male\",\"name\",\"street\",\"_start\",\"_end\",\"_type\"\n"
                    + "\"0\",\":User:User1\",\"42\",\"\",\"[\"\"a\"\",\"\"b\"\",\"\"c\"\"]\",\"true\",\"foo\",\"\",,,\n"
                    + "\"1\",\":User\",\"42\",\"\",\"\",\"\",\"bar\",\"\",,,\n"
                    + "\"2\",\":User\",\"12\",\"\",\"\",\"\",\"\",\"\",,,\n"
                    + "\"3\",\":Address:Address1\",\"\",\"Milano\",\"\",\"\",\"Andrea\",\"Via Garibaldi, 7\",,,\n"
                    + "\"4\",\":Address\",\"\",\"\",\"\",\"\",\"Bar Sport\",\"\",,,\n"
                    + "\"5\",\":Address\",\"\",\"\",\"\",\"\",\"\",\"via Benni\",,,\n"
                    + ",,,,,,,,\"0\",\"1\",\"KNOWS\"\n"
                    + ",,,,,,,,\"3\",\"4\",\"NEXT_DELIVERY\"\n";

    private static final File directory = new File("target/import");

    static {
        //noinspection ResultOfMethodCallIgnored
        directory.mkdirs();
    }

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule()
            .withSetting(
                    GraphDatabaseSettings.load_csv_file_url_root,
                    directory.toPath().toAbsolutePath())
            .withSetting(ApocSettings.apoc_export_file_enabled, true)
            .withSetting(ApocSettings.apoc_import_file_enabled, true);

    @BeforeClass
    public static void setUp() throws Exception {
        TestUtil.registerProcedure(db, ExportCSV.class, Graphs.class, Meta.class, ImportCsv.class);
        db.executeTransactionally(
                "CREATE (f:User1:User {name:'foo',age:42,male:true,kids:['a','b','c']})-[:KNOWS]->(b:User {name:'bar',age:42}),(c:User {age:12})");
        db.executeTransactionally(
                "CREATE (f:Address1:Address {name:'Andrea', city: 'Milano', street:'Via Garibaldi, 7'})-[:NEXT_DELIVERY]->(a:Address {name: 'Bar Sport'}), (b:Address {street: 'via Benni'})");
    }

    @AfterClass
    public static void teardown() {
        db.shutdown();
    }

    private String readFile(String fileName) {
        return readFile(fileName, UTF_8, CompressionAlgo.NONE);
    }

    private String readFile(String fileName, Charset charset, CompressionAlgo compression) {
        return BinaryTestUtil.readFileToString(new File(directory, fileName), charset, compression);
    }

    @Test
    public void testExportInvalidQuoteValue() {
        try {
            String fileName = "all.csv";
            TestUtil.testCall(
                    db,
                    "CALL apoc.export.csv.all($file,{quotes: 'Invalid'})",
                    map("file", fileName),
                    (r) -> assertResults(fileName, r, "database"));
            fail();
        } catch (RuntimeException e) {
            final String expectedMessage =
                    "Failed to invoke procedure `apoc.export.csv.all`: Caused by: java.lang.RuntimeException: The string value of the field quote is not valid";
            assertEquals(expectedMessage, e.getMessage());
        }
    }

    @Test
    public void testExportAllCsvCompressed() {
        final CompressionAlgo compressionAlgo = DEFLATE;
        String fileName = "all.csv.zz";
        TestUtil.testCall(
                db,
                "CALL apoc.export.csv.all($file, $config)",
                map("file", fileName, "config", map("compression", compressionAlgo.name())),
                (r) -> assertResults(fileName, r, "database"));
        assertEquals(EXPECTED, readFile(fileName, UTF_8, compressionAlgo));
    }

    @Test
    public void testCsvRoundTrip() {
        db.executeTransactionally(
                "CREATE (f:Roundtrip {name:'foo',age:42,male:true,kids:['a','b','c']}),(b:Roundtrip {name:'bar',age:42}),(c:Roundtrip {age:12})");

        String fileName = "separatedFiles.csv.gzip";
        final Map<String, Object> params = map(
                "file",
                fileName,
                "query",
                "MATCH (u:Roundtrip) return u.name as name",
                "config",
                map(CompressionConfig.COMPRESSION, GZIP.name()));
        TestUtil.testCall(
                db,
                "CALL apoc.export.csv.query($query, $file, $config)",
                params,
                (r) -> assertEquals(fileName, r.get("file")));

        final String deleteQuery = "MATCH (n:Roundtrip) DETACH DELETE n";
        db.executeTransactionally(deleteQuery);

        TestUtil.testCall(
                db,
                "CALL apoc.import.csv([{fileName: $file, labels: ['Roundtrip']}], [], $config) ",
                params,
                r -> assertEquals(3L, r.get("nodes")));

        TestUtil.testResult(db, "MATCH (n:Roundtrip) return n.name as name", r -> {
            final Set<String> actual = Iterators.asSet(r.columnAs("name"));
            assertEquals(Set.of("foo", "bar", ""), actual);
        });

        db.executeTransactionally(deleteQuery);
    }

    @Test
    public void testCsvBackslashes() {
        db.executeTransactionally("CREATE (n:Test {name: 'Test', value: '{\"new\":\"4\\'10\\\\\\\"\"}'})");

        String fileName = "test.csv.quotes.csv";
        final Map<String, Object> params =
                map("file", fileName, "query", "MATCH (n: Test) RETURN n", "config", map("quotes", "always"));

        TestUtil.testCall(
                db, "CALL apoc.export.csv.all($file, $config)", params, (r) -> assertEquals(fileName, r.get("file")));

        final String deleteQuery = "MATCH (n:Test) DETACH DELETE n";
        db.executeTransactionally(deleteQuery);

        TestUtil.testCall(
                db,
                "CALL apoc.import.csv([{fileName: $file, labels: ['Test']}],[],{})",
                params,
                r -> assertEquals(9L, r.get("nodes")));

        TestUtil.testResult(db, "MATCH (n:Test) RETURN n.name as name, n.value as value", r -> {
            var nodes = r.stream().filter(node -> node.get("name").equals("Test"));
            var actual = nodes.map(node -> (String) node.get("value")).collect(Collectors.toSet());
            assertEquals(Set.of("{\"new\":\"4'10\\\"\"}"), actual);
        });

        db.executeTransactionally(deleteQuery);
    }

    @Test
    public void testExportAllCsv() {
        String fileName = "all.csv";
        testExportCsvAllCommon(fileName);
    }

    @Test
    public void testExportAllCsvWithDotInName() {
        String fileName = "all.with.dot.filename.csv";
        testExportCsvAllCommon(fileName);
    }

    @Test
    public void testExportAllCsvWithoutExtension() {
        String fileName = "all";
        testExportCsvAllCommon(fileName);
    }

    private void testExportCsvAllCommon(String fileName) {
        TestUtil.testCall(
                db,
                "CALL apoc.export.csv.all($file,null)",
                map("file", fileName),
                (r) -> assertResults(fileName, r, "database"));
        assertEquals(EXPECTED, readFile(fileName));
    }

    @Test
    public void testExportAllCsvWithSample() throws IOException {
        db.executeTransactionally(
                "CREATE (:User:Sample {`last:Name`:'Galilei'}), (:User:Sample {address:'Universe'}),\n"
                        + "(:User:Sample {foo:'bar'})-[:KNOWS {one: 'two', three: 'four'}]->(:User:Sample {baz:'baa', foo: true})");
        String fileName = "all.csv";
        final long totalNodes = 10L;
        final long totalRels = 3L;
        final long totalProps = 19L;
        TestUtil.testCall(
                db,
                "CALL apoc.export.csv.all($file, null)",
                map("file", fileName),
                (r) -> assertResults(fileName, r, "database", totalNodes, totalRels, totalProps, true));
        assertEquals(EXP_SAMPLE, readFile(fileName));

        // quotes: 'none' to simplify header testing
        TestUtil.testCall(
                db,
                "CALL apoc.export.csv.all($file, {sampling: true, samplingConfig: {sample: 1}, quotes: 'none'})",
                map("file", fileName),
                (r) -> assertResults(fileName, r, "database", totalNodes, totalRels, totalProps, false));

        final String[] s = Files.lines(new File(directory, fileName).toPath())
                .findFirst()
                .get()
                .split(",");
        assertTrue(s.length < 17);
        assertTrue(Arrays.asList(s).containsAll(List.of("_id", "_labels", "_start", "_end", "_type")));

        db.executeTransactionally("MATCH (n:Sample) DETACH DELETE n");
    }

    @Test
    public void testExportAllCsvWithQuotes() {
        String fileName = "all.csv";
        TestUtil.testCall(
                db,
                "CALL apoc.export.csv.all($file,{quotes: true})",
                map("file", fileName),
                (r) -> assertResults(fileName, r, "database"));
        assertEquals(EXPECTED, readFile(fileName));
    }

    @Test
    public void testExportAllCsvWithoutQuotes() {
        String fileName = "all.csv";
        TestUtil.testCall(
                db,
                "CALL apoc.export.csv.all($file,{quotes: 'none'})",
                map("file", fileName),
                (r) -> assertResults(fileName, r, "database"));
        assertEquals(EXPECTED_NONE_QUOTES, readFile(fileName));
    }

    @Test
    public void testExportAllCsvNeededQuotes() {
        String fileName = "all.csv";
        TestUtil.testCall(
                db,
                "CALL apoc.export.csv.all($file,{quotes: 'ifNeeded'})",
                map("file", fileName),
                (r) -> assertResults(fileName, r, "database"));
        assertEquals(EXPECTED_NEEDED_QUOTES, readFile(fileName));
    }

    @Test
    public void testExportAllCsvAlwaysQuotes() {
        String fileName = "all.csv";
        TestUtil.testCall(
                db,
                "CALL apoc.export.csv.all($file,{quotes: 'always'})",
                map("file", fileName),
                (r) -> assertResults(fileName, r, "database"));
        assertEquals(EXPECTED_QUOTES_ALWAYS, readFile(fileName));
    }

    @Test
    public void testExportGraphCsv() {
        String fileName = "graph.csv";
        TestUtil.testCall(
                db,
                "CALL apoc.graph.fromDB('test',{}) yield graph "
                        + "CALL apoc.export.csv.graph(graph, $file,{quotes: 'none'}) "
                        + "YIELD nodes, relationships, properties, file, source,format, time "
                        + "RETURN *",
                map("file", fileName),
                (r) -> assertResults(fileName, r, "graph"));
        assertEquals(EXPECTED_NONE_QUOTES, readFile(fileName));
    }

    @Test
    public void testExportGraphCsvWithoutQuotes() {
        String fileName = "graph.csv";
        TestUtil.testCall(
                db,
                "CALL apoc.graph.fromDB('test',{}) yield graph " + "CALL apoc.export.csv.graph(graph, $file,null) "
                        + "YIELD nodes, relationships, properties, file, source,format, time "
                        + "RETURN *",
                map("file", fileName),
                (r) -> assertResults(fileName, r, "graph"));
        assertEquals(EXPECTED, readFile(fileName));
    }

    @Test
    public void testExportQueryCsv() {
        String fileName = "query.csv";
        String query = "MATCH (u:User) return u.age, u.name, u.male, u.kids, labels(u)";
        TestUtil.testCall(
                db, "CALL apoc.export.csv.query($query,$file,null)", map("file", fileName, "query", query), (r) -> {
                    assertTrue(
                            "Should get statement", r.get("source").toString().contains("statement: cols(5)"));
                    assertEquals(fileName, r.get("file"));
                    assertEquals("csv", r.get("format"));
                });
        assertEquals(EXPECTED_QUERY, readFile(fileName));
    }

    @Test
    public void testExportQueryCsvWithoutQuotes() {
        String fileName = "query.csv";
        String query = "MATCH (u:User) return u.age, u.name, u.male, u.kids, labels(u)";
        TestUtil.testCall(
                db,
                "CALL apoc.export.csv.query($query,$file,{quotes: false})",
                map("file", fileName, "query", query),
                (r) -> {
                    assertTrue(
                            "Should get statement", r.get("source").toString().contains("statement: cols(5)"));
                    assertEquals(fileName, r.get("file"));
                    assertEquals("csv", r.get("format"));
                });
        assertEquals(EXPECTED_QUERY_WITHOUT_QUOTES, readFile(fileName));
    }

    @Test
    public void testExportCsvAdminOperationErrorMessage() {
        String filename = "test.csv";
        List<String> invalidQueries =
                List.of("SHOW CONSTRAINTS YIELD id, name, type RETURN *", "SHOW INDEXES YIELD id, name, type RETURN *");

        for (String query : invalidQueries) {
            QueryExecutionException e = Assert.assertThrows(
                    QueryExecutionException.class,
                    () -> TestUtil.testCall(
                            db,
                            "" + "CALL apoc.export.csv.query(" + "$query," + "$file," + "{quotes: false}" + ")",
                            map("query", query, "file", filename),
                            (r) -> {}));

            assertError(e, INVALID_QUERY_MODE_ERROR, RuntimeException.class, "apoc.export.csv.query");
        }
    }

    @Test
    public void testExportQueryNodesCsv() {
        String fileName = "query_nodes.csv";
        String query = "MATCH (u:User) return u";
        TestUtil.testCall(
                db, "CALL apoc.export.csv.query($query,$file,null)", map("file", fileName, "query", query), (r) -> {
                    assertTrue(
                            "Should get statement", r.get("source").toString().contains("statement: cols(1)"));
                    assertEquals(fileName, r.get("file"));
                    assertEquals("csv", r.get("format"));
                });
        assertEquals(EXPECTED_QUERY_NODES, readFile(fileName));
    }

    @Test
    public void testExportQueryNodesCsvParams() {
        String fileName = "query_nodes.csv";
        String query = "MATCH (u:User) WHERE u.age > $age return u";
        TestUtil.testCall(
                db,
                "CALL apoc.export.csv.query($query,$file,{params:{age:10}})",
                map("file", fileName, "query", query),
                (r) -> {
                    assertTrue(
                            "Should get statement", r.get("source").toString().contains("statement: cols(1)"));
                    assertEquals(fileName, r.get("file"));
                    assertEquals("csv", r.get("format"));
                });
        assertEquals(EXPECTED_QUERY_NODES, readFile(fileName));
    }

    private void assertResults(String fileName, Map<String, Object> r, final String source) {
        assertResults(fileName, r, source, 6L, 2L, 12L, true);
    }

    private void assertResults(
            String fileName,
            Map<String, Object> r,
            final String source,
            Long expectedNodes,
            Long expectedRelationships,
            Long expectedProperties,
            boolean assertPropEquality) {
        assertEquals(expectedNodes, r.get("nodes"));
        assertEquals(expectedRelationships, r.get("relationships"));
        if (assertPropEquality) {
            assertEquals(expectedProperties, r.get("properties"));
        } else {
            assertTrue((Long) r.get("properties") < expectedProperties);
        }
        final String expectedSource = source + ": nodes(" + expectedNodes + "), rels(" + expectedRelationships + ")";
        assertEquals(expectedSource, r.get("source"));
        assertCsvCommon(fileName, r);
    }

    private void assertCsvCommon(String fileName, Map<String, Object> r) {
        assertEquals(fileName, r.get("file"));
        assertEquals("csv", r.get("format"));
        assertTrue("Should get time greater than 0", ((long) r.get("time")) >= 0);
    }

    @Test
    public void testExportAllCsvStreaming() {
        String statement = "CALL apoc.export.csv.all(null,{stream:true,batchSize:2})";
        assertExportStreaming(statement, NONE);
    }

    @Test
    public void testExportAllCsvStreamingCompressed() {
        final CompressionAlgo algo = GZIP;
        String statement =
                "CALL apoc.export.csv.all(null, {compression: '" + algo.name() + "',stream:true,batchSize:2})";
        assertExportStreaming(statement, algo);
    }

    private void assertExportStreaming(String statement, CompressionAlgo algo) {
        StringBuilder sb = new StringBuilder();
        testResult(db, statement, (res) -> {
            Map<String, Object> r = res.next();
            assertEquals(2L, r.get("batchSize"));
            assertEquals(1L, r.get("batches"));
            assertEquals(2L, r.get("nodes"));
            assertEquals(2L, r.get("rows"));
            assertEquals(0L, r.get("relationships"));
            assertEquals(6L, r.get("properties"));
            assertNull("Should get file", r.get("file"));
            assertEquals("csv", r.get("format"));
            assertTrue("Should get time greater than 0", ((long) r.get("time")) >= 0);
            sb.append(getDecompressedData(algo, r.get("data")));
            r = res.next();
            assertEquals(2L, r.get("batchSize"));
            assertEquals(2L, r.get("batches"));
            assertEquals(4L, r.get("nodes"));
            assertEquals(4L, r.get("rows"));
            assertEquals(0L, r.get("relationships"));
            assertEquals(10L, r.get("properties"));
            assertTrue("Should get time greater than 0", ((long) r.get("time")) >= 0);
            sb.append(getDecompressedData(algo, r.get("data")));
            r = res.next();
            assertEquals(2L, r.get("batchSize"));
            assertEquals(3L, r.get("batches"));
            assertEquals(6L, r.get("nodes"));
            assertEquals(6L, r.get("rows"));
            assertEquals(0L, r.get("relationships"));
            assertEquals(12L, r.get("properties"));
            assertTrue("Should get time greater than 0", ((long) r.get("time")) >= 0);
            sb.append(getDecompressedData(algo, r.get("data")));
            r = res.next();
            assertEquals(2L, r.get("batchSize"));
            assertEquals(4L, r.get("batches"));
            assertEquals(6L, r.get("nodes"));
            assertEquals(8L, r.get("rows"));
            assertEquals(2L, r.get("relationships"));
            assertEquals(12L, r.get("properties"));
            assertTrue("Should get time greater than 0", ((long) r.get("time")) >= 0);
            sb.append(getDecompressedData(algo, r.get("data")));
            res.close();
        });
        assertEquals(EXPECTED, sb.toString());
    }

    @Test
    public void testCypherCsvStreaming() {
        String query = "MATCH (u:User) return u.age, u.name, u.male, u.kids, labels(u)";
        StringBuilder sb = new StringBuilder();
        testResult(
                db,
                "CALL apoc.export.csv.query($query,null,{stream:true,batchSize:2})",
                map("query", query),
                getAndCheckStreamingMetadataQueryMatchUsers(sb));
        assertEquals(EXPECTED_QUERY, sb.toString());
    }

    @Test
    public void testCypherCsvStreamingWithoutQuotes() {
        String query = "MATCH (u:User) return u.age, u.name, u.male, u.kids, labels(u)";
        StringBuilder sb = new StringBuilder();
        testResult(
                db,
                "CALL apoc.export.csv.query($query,null,{quotes: false, stream:true,batchSize:2})",
                map("query", query),
                getAndCheckStreamingMetadataQueryMatchUsers(sb));

        assertEquals(EXPECTED_QUERY_WITHOUT_QUOTES, sb.toString());
    }

    private Consumer<Result> getAndCheckStreamingMetadataQueryMatchUsers(StringBuilder sb) {
        return (res) -> {
            Map<String, Object> r = res.next();
            assertEquals(2L, r.get("batchSize"));
            assertEquals(1L, r.get("batches"));
            assertEquals(0L, r.get("nodes"));
            assertEquals(2L, r.get("rows"));
            assertEquals(0L, r.get("relationships"));
            assertEquals(10L, r.get("properties"));
            assertNull("Should get file", r.get("file"));
            assertEquals("csv", r.get("format"));
            assertTrue("Should get time greater than 0", ((long) r.get("time")) >= 0);
            sb.append(r.get("data"));
            r = res.next();
            assertEquals(2L, r.get("batchSize"));
            assertEquals(2L, r.get("batches"));
            assertEquals(0L, r.get("nodes"));
            assertEquals(3L, r.get("rows"));
            assertEquals(0L, r.get("relationships"));
            assertEquals(15L, r.get("properties"));
            assertTrue("Should get time greater than 0", ((long) r.get("time")) >= 0);
            sb.append(r.get("data"));
        };
    }

    @Test
    public void testCypherCsvStreamingWithAlwaysQuotes() {
        String query = "MATCH (a:Address) return a.name, a.city, a.street, labels(a)";
        StringBuilder sb = new StringBuilder();
        testResult(
                db,
                "CALL apoc.export.csv.query($query,null,{quotes: 'always', stream:true,batchSize:2})",
                map("query", query),
                getAndCheckStreamingMetadataQueryMatchAddress(sb));

        assertEquals(EXPECTED_QUERY_QUOTES_ALWAYS, sb.toString());
    }

    @Test
    public void testCypherCsvStreamingWithNeededQuotes() {
        String query = "MATCH (a:Address) return a.name, a.city, a.street, labels(a)";
        StringBuilder sb = new StringBuilder();
        testResult(
                db,
                "CALL apoc.export.csv.query($query,null,{quotes: 'ifNeeded', stream:true,batchSize:2})",
                map("query", query),
                getAndCheckStreamingMetadataQueryMatchAddress(sb));

        assertEquals(EXPECTED_QUERY_QUOTES_NEEDED, sb.toString());
    }

    @Test
    public void testCypherCsvStreamingWithNoneQuotes() {
        String query = "MATCH (a:Address) return a.name, a.city, a.street, labels(a)";
        StringBuilder sb = new StringBuilder();
        testResult(
                db,
                "CALL apoc.export.csv.query($query,null,{quotes: 'none', stream:true,batchSize:2})",
                map("query", query),
                getAndCheckStreamingMetadataQueryMatchAddress(sb));

        assertEquals(EXPECTED_QUERY_QUOTES_NONE, sb.toString());
    }

    @Test
    public void testExportQueryCsvIssue1188() {
        String copyright = "\n"
                + "(c) 2018 Hovsepian, Albanese, et al. \"\"ASCB(r),\"\" \"\"The American Society for Cell Biology(r),\"\" and \"\"Molecular Biology of the Cell(r)\"\" are registered trademarks of The American Society for Cell Biology.\n"
                + "2018\n"
                + "\n"
                + "This article is distributed by The American Society for Cell Biology under license from the author(s). Two months after publication it is available to the public under an Attribution-Noncommercial-Share Alike 3.0 Unported Creative Commons License.\n"
                + "\n";
        String pk = "5921569";
        db.executeTransactionally(
                "CREATE (n:Document{pk:$pk, copyright: $copyright})", map("copyright", copyright, "pk", pk));
        String query = "MATCH (n:Document{pk:'5921569'}) return n.pk as pk, n.copyright as copyright";
        TestUtil.testCall(
                db,
                "CALL apoc.export.csv.query($query, null, $config)",
                map("query", query, "config", map("stream", true)),
                (r) -> {
                    List<String[]> csv = CsvTestUtil.toCollection(r.get("data").toString());
                    assertEquals(2, csv.size());
                    assertArrayEquals(new String[] {"pk", "copyright"}, csv.get(0));
                    assertArrayEquals(new String[] {"5921569", copyright}, csv.get(1));
                });
        db.executeTransactionally("MATCH (d:Document) DETACH DELETE d");
    }

    @Test
    public void testExportWgsPoint() {
        db.executeTransactionally(
                "CREATE (p:Position {place: point({latitude: 12.78, longitude: 56.7, height: 1.1})})");

        TestUtil.testCall(
                db,
                "CALL apoc.export.csv.query($query, null, {quotes: 'none', stream: true}) YIELD data RETURN data",
                map("query", "MATCH (p:Position) RETURN p.place as place"),
                (r) -> {
                    String data = (String) r.get("data");
                    Map<String, Object> place = Util.fromJson(data.split(System.lineSeparator())[1], Map.class);
                    assertEquals(12.78D, (double) place.get("latitude"), 0);
                    assertEquals(56.7D, (double) place.get("longitude"), 0);
                    assertEquals(1.1D, (double) place.get("height"), 0);
                });
        db.executeTransactionally("MATCH (n:Position) DETACH DELETE n");
    }

    @Test
    public void testGithubIssue4120() {
        String query = "UNWIND [\"\\\"Jan\\\" Apoc User\", \"\\\"Jan\\\" Apoc\\\"\\\" User\"] AS name RETURN name";
        String expected = "name\n" + "\"\"\"Jan\"\" Apoc User\"\n" + "\"\"\"Jan\"\" Apoc\"\"\"\" User\"\n";
        StringBuilder sb = new StringBuilder();
        testResult(
                db,
                "CALL apoc.export.csv.query($query,null,{quotes: 'ifNeeded', stream:true,batchSize:2})",
                map("query", query),
                (res) -> {
                    Map<String, Object> r = res.next();
                    assertEquals(2L, r.get("batchSize"));
                    assertEquals(1L, r.get("batches"));
                    assertEquals(0L, r.get("nodes"));
                    assertEquals(2L, r.get("rows"));
                    assertEquals(0L, r.get("relationships"));
                    assertEquals(2L, r.get("properties"));
                    assertNull("Should get file", r.get("file"));
                    assertEquals("csv", r.get("format"));
                    assertTrue("Should get time greater than 0", ((long) r.get("time")) >= 0);
                    sb.append(r.get("data"));
                    r = res.next();
                    assertEquals(2L, r.get("batchSize"));
                    assertEquals(2L, r.get("batches"));
                    assertEquals(0L, r.get("nodes"));
                    assertEquals(2L, r.get("rows"));
                    assertEquals(0L, r.get("relationships"));
                    assertEquals(2L, r.get("properties"));
                    assertTrue("Should get time greater than 0", ((long) r.get("time")) >= 0);
                    sb.append(r.get("data"));
                });

        assertEquals(expected, sb.toString());
    }

    private Consumer<Result> getAndCheckStreamingMetadataQueryMatchAddress(StringBuilder sb) {
        return (res) -> {
            Map<String, Object> r = res.next();
            assertEquals(2L, r.get("batchSize"));
            assertEquals(1L, r.get("batches"));
            assertEquals(0L, r.get("nodes"));
            assertEquals(2L, r.get("rows"));
            assertEquals(0L, r.get("relationships"));
            assertEquals(8L, r.get("properties"));
            assertNull("Should get file", r.get("file"));
            assertEquals("csv", r.get("format"));
            assertTrue("Should get time greater than 0", ((long) r.get("time")) >= 0);
            sb.append(r.get("data"));
            r = res.next();
            assertEquals(2L, r.get("batchSize"));
            assertEquals(2L, r.get("batches"));
            assertEquals(0L, r.get("nodes"));
            assertEquals(3L, r.get("rows"));
            assertEquals(0L, r.get("relationships"));
            assertEquals(12L, r.get("properties"));
            assertTrue("Should get time greater than 0", ((long) r.get("time")) >= 0);
            sb.append(r.get("data"));
        };
    }
}
