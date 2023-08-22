package apoc.export.parquet;

import apoc.util.collection.Iterators;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;
import org.neo4j.values.storable.DateValue;
import org.neo4j.values.storable.LocalDateTimeValue;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static apoc.ApocConfig.APOC_EXPORT_FILE_ENABLED;
import static apoc.ApocConfig.APOC_IMPORT_FILE_ENABLED;
import static apoc.ApocConfig.LOAD_FROM_FILE_ERROR;
import static apoc.ApocConfig.apocConfig;
import static apoc.export.parquet.ExportParquet.EXPORT_TO_FILE_PARQUET_ERROR;
import static apoc.export.parquet.ParquetTestUtil.assertBarRel;
import static apoc.export.parquet.ParquetTestUtil.assertNodeAndLabel;
import static apoc.export.parquet.ParquetTestUtil.beforeClassCommon;
import static apoc.export.parquet.ParquetTestUtil.beforeCommon;
import static apoc.export.parquet.ParquetTestUtil.testImportAllCommon;
import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testResult;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


public class ParquetTest {

    public static final Map<String, Map<String, String>> MAPPING_ALL = Map.of("mapping",
            Map.of("bffSince", "Duration", "place", "Point",
                    "listDate", "DateArray", "listInt", "LongArray")
    );
    public static final Map<String, Map<String, String>> MAPPING_QUERY = Map.of("mapping",
            Map.of("n", "Node", "r", "Relationship", "o", "Node")
    );
    private static File directory = new File("target/parquet import");
    static { //noinspection ResultOfMethodCallIgnored
        directory.mkdirs();
    }

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule()
            .withSetting(GraphDatabaseSettings.load_csv_file_url_root, directory.toPath().toAbsolutePath());

    @BeforeClass
    public static void beforeClass() {
        beforeClassCommon(db);
    }

    @Before
    public void before() {
        beforeCommon(db);
    }

    @Test
    public void testStreamRoundtripParquetQueryMultipleTypes() {
        List<Object> values = List.of(1L, "", 7.0, DateValue.parse("1999"), LocalDateTimeValue.parse("2023-06-14T08:38:28.193000000"));

        final byte[] byteArray = db.executeTransactionally(
                "CALL apoc.export.parquet.query.stream('UNWIND $values AS item RETURN item', {params: {values: $values}})",
                Map.of("values", values),
                ParquetTest::extractByteArray);

        // then
        final String query = "CALL apoc.load.parquet($byteArray, $config) YIELD value " +
                             "RETURN value";
        testResult(db, query, Map.of("byteArray", byteArray, "config", MAPPING_QUERY), result -> {
            List<Map<String, Object>> value = Iterators.asList(result.columnAs("value"));
            Set<String> expected = Set.of("", "1", "7.0", "2023-06-14T08:38:28.193", "1999-01-01");
            Set<Object> actual = value.stream()
                    .flatMap(i -> i.values().stream())
                    .collect(Collectors.toSet());
            assertEquals(expected, actual);
        });
    }

    @Test
    public void testFileRoundtripParquetGraph() {
        // given - when
        String file = db.executeTransactionally("CALL apoc.graph.fromDB('neo4j',{}) yield graph " +
                        "CALL apoc.export.parquet.graph(graph, 'graph_test.parquet') YIELD file " +
                        "RETURN file",
                Map.of(),
                ParquetTestUtil::extractFileName);

        // then
        final String query = "CALL apoc.load.parquet($file, $config) YIELD value " +
                "RETURN value";
        testResult(db, query, Map.of("file", file, "config", MAPPING_ALL),
                ParquetTestUtil::roundtripLoadAllAssertions);
    }

    @Test
    public void testStreamRoundtripParquetAllWithImportExportConfsDisabled() {
        // disable both export and import configs
        apocConfig().setProperty(APOC_IMPORT_FILE_ENABLED, false);
        apocConfig().setProperty(APOC_EXPORT_FILE_ENABLED, false);

        // should work regardless of the previous config
        testStreamRoundtripAllCommon();
    }

    @Test
    public void testExportFileWithConfigDisabled() {
        apocConfig().setProperty(APOC_EXPORT_FILE_ENABLED, false);

        assertFails("CALL apoc.export.parquet.all('ignore.parquet')", EXPORT_TO_FILE_PARQUET_ERROR);
    }

    @Test
    public void testLoadImportFiletWithConfigDisabled() {
        apocConfig().setProperty(APOC_IMPORT_FILE_ENABLED, false);

        assertFails("CALL apoc.load.parquet('ignore.parquet')", LOAD_FROM_FILE_ERROR);
        assertFails("CALL apoc.import.parquet('ignore.parquet')", LOAD_FROM_FILE_ERROR);
    }

    private static void assertFails(String call, String expectedErrMsg) {
        try {
            testCall(db, call, r -> fail("Should fail due to " + expectedErrMsg));
        } catch (Exception e) {
            String actualErrMsg = e.getMessage();
            assertTrue("Actual err. message is: " + actualErrMsg, actualErrMsg.contains(expectedErrMsg));
        }
    }

    @Test
    public void testStreamRoundtripParquetAll() {
        testStreamRoundtripAllCommon();
    }

    private static void testStreamRoundtripAllCommon() {
        // given - when
        final byte[] bytes = db.executeTransactionally("CALL apoc.export.parquet.all.stream()",
                Map.of(),
                ParquetTest::extractByteArray);

        // then
        final String query = "CALL apoc.load.parquet($bytes, $config) YIELD value " +
                             "RETURN value";

        testResult(db, query, Map.of("bytes", bytes, "config", MAPPING_ALL),
                ParquetTestUtil::roundtripLoadAllAssertions);
    }

    @Test
    public void testStreamRoundtripWithAnotherpleBatches() {
        final List<byte[]> bytes = db.executeTransactionally("CALL apoc.export.parquet.all.stream({batchSize:1})",
                Map.of(),
                r -> Iterators.asList(r.columnAs("value")));

        // then
        final String query = "UNWIND $bytes AS byte CALL apoc.load.parquet(byte, $config) YIELD value " +
                             "RETURN value";

        testResult(db, query, Map.of("bytes", bytes, "config", MAPPING_ALL),
                ParquetTestUtil::roundtripLoadAllAssertions);
    }

    @Test
    public void testRoundtripWithMultipleBatches() {
        final String fileName = db.executeTransactionally("CALL apoc.export.parquet.all('test.parquet', {batchSize:1})",
                Map.of(),
                ParquetTestUtil::extractFileName);

        // then
        final String query = "CALL apoc.load.parquet($file, $config) YIELD value " +
                             "RETURN value";

        testResult(db, query, Map.of("file", fileName, "config", MAPPING_ALL),
                ParquetTestUtil::roundtripLoadAllAssertions);
    }

    @Test
    public void testFileRoundtripImportParquetAll() {
        // given - when
        String file = db.executeTransactionally("CALL apoc.export.parquet.all('test_all.parquet') YIELD file",
                Map.of(),
                ParquetTestUtil::extractFileName);


        // then
        Map<String, Object> params = Map.of("file", file, "config", MAPPING_ALL);
        testImportAllCommon(db, params);
    }

    @Test
    public void testFileRoundtripParquetAll() {
        // given - when
        String file = db.executeTransactionally("CALL apoc.export.parquet.all('test_all.parquet') YIELD file",
                Map.of(),
                ParquetTestUtil::extractFileName);

        // then
        final String query = "CALL apoc.load.parquet($file, $config) YIELD value " +
                "RETURN value";

        testResult(db, query, Map.of("file", file,  "config", MAPPING_ALL),
                ParquetTestUtil::roundtripLoadAllAssertions);
    }

    @Test
    public void testReturnNodeAndRelStream() {
        testReturnNodeAndRelCommon(() -> db.executeTransactionally("CALL apoc.export.parquet.query.stream('MATCH (n:ParquetNode)-[r:BAR]->(o:Other) RETURN n,r,o ORDER BY n.idStart') ",
                Map.of(),
                ParquetTest::extractByteArray));
    }

    @Test
    public void testReturnNodeAndRel() {
        testReturnNodeAndRelCommon(() -> db.executeTransactionally(
                "CALL apoc.export.parquet.query('MATCH (n:ParquetNode)-[r:BAR]->(o:Other) RETURN n,r,o ORDER BY n.idStart', " +
                "'volume_test.parquet', $config) YIELD file ",
                Map.of("config", MAPPING_QUERY),
                ParquetTestUtil::extractFileName));
    }

    public static void testReturnNodeAndRelCommon(Supplier<Object> supplier) {
        db.executeTransactionally("CREATE (:ParquetNode{idStart:1})-[:BAR {idRel: 'one'}]->(:Other {idOther: datetime('2020')})");
        db.executeTransactionally("CREATE (:ParquetNode{idStart:2})-[:BAR {idRel: 'two'}]->(:Other {idOther: datetime('1999')})");

        Object fileOrBinary = supplier.get();

        // then
        final String query = "CALL apoc.load.parquet($file, $config)";

        testResult(db, query, Map.of("file", fileOrBinary, "config", MAPPING_QUERY),
                res -> {
                    ResourceIterator<Map<String, Object>> value = res.columnAs("value");
                    Map<String, Object> row = value.next();
                    Map<String, Object> relTwo = (Map<String, Object>) row.get("r");
                    assertBarRel("one", relTwo);

                    Map<String, Object> startTwo = (Map<String, Object>) row.get("n");
                    assertNodeAndLabel(startTwo, "ParquetNode");
                    assertEquals(1L, startTwo.get("idStart"));

                    Map<String, Object> endTwo = (Map<String, Object>) row.get("o");
                    assertNodeAndLabel(endTwo, "Other");
                    assertEquals("2020-01-01T00:00Z", endTwo.get("idOther"));

                    row = value.next();
                    Map<String, Object> rel = (Map<String, Object>) row.get("r");
                    assertBarRel("two", rel);

                    Map<String, Object> start = (Map<String, Object>) row.get("n");
                    assertNodeAndLabel(start, "ParquetNode");
                    assertEquals(2L, start.get("idStart"));

                    Map<String, Object> end = (Map<String, Object>) row.get("o");
                    assertNodeAndLabel(end, "Other");
                    assertEquals("1999-01-01T00:00Z", end.get("idOther"));

                    assertFalse(res.hasNext());
                });

        db.executeTransactionally("MATCH (n:ParquetNode), (o:Other) DETACH DELETE n, o");
    }

    @Test
    public void testFileVolumeParquetAll() {
        // given - when
        db.executeTransactionally("UNWIND range(0, 10000 - 1) AS id CREATE (:ParquetNode{id:id})");

        String file = db.executeTransactionally("CALL apoc.export.parquet.query('MATCH (n:ParquetNode) RETURN n.id AS id', 'volume_test.parquet') YIELD file ",
                Map.of(),
                ParquetTestUtil::extractFileName);

        final List<Long> expected = LongStream.range(0, 10000)
                .boxed()
                .collect(Collectors.toList());

        // then
        final String query = "CALL apoc.load.parquet($file, $config) YIELD value " +
                "WITH value.id AS id ORDER BY id RETURN collect(id) as ids";

        testCall(db, query, Map.of("file", file, "config", MAPPING_ALL),
                r -> assertEquals(expected, r.get("ids")));

        db.executeTransactionally("MATCH (n:ParquetNode) DELETE n");
    }

    private static byte[] extractByteArray(Result result) {
        ResourceIterator<byte[]> value = result.columnAs("value");
        return value.next();
    }

}