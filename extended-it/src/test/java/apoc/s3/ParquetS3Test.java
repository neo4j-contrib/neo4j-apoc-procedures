package apoc.s3;

import apoc.export.parquet.ParquetTestUtil;
import apoc.util.collection.Iterators;
import apoc.util.s3.S3BaseTest;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.io.File;
import java.util.Collections;
import java.util.Map;

import static apoc.export.parquet.ParquetTest.MAPPING_ALL;
import static apoc.export.parquet.ParquetTestUtil.beforeClassCommon;
import static apoc.export.parquet.ParquetTestUtil.beforeCommon;
import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testResult;
import static apoc.util.s3.S3Util.putToS3AndGetUrl;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ParquetS3Test extends S3BaseTest {

    private final String EXPORT_FILENAME = "test_all.parquet";

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    @BeforeClass
    public static void beforeClass() {
        beforeClassCommon(db);
    }

    @Before
    public void before() {
        beforeCommon(db);
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

        deleteFile(file.replace("file://", ""));
    }

    @Test
    public void testFileRoundtripParquetAllFromS3Url() {
        // given - when
        String filename = exportToParquetFile(EXPORT_FILENAME);
        String url = putToS3AndGetUrl(s3Container, filename);

        // then
        final String query = "CALL apoc.load.parquet($url, $config) YIELD value " +
                "RETURN value";

        testResult(db, query, Map.of("url", url,  "config", MAPPING_ALL),
                ParquetTestUtil::roundtripLoadAllAssertions);

        deleteFile(filename);
    }

    @Test
    public void testImportParquetFromS3Url() {
        String filename = exportToParquetFile(EXPORT_FILENAME);
        String url = putToS3AndGetUrl(s3Container, filename);

        db.executeTransactionally("MATCH (n) DETACH DELETE n");
        Long count = db.executeTransactionally("MATCH (n) RETURN count(n) AS count", Collections.emptyMap(),
                result -> Iterators.single(result.columnAs("count")));
        assertEquals(0L, count.longValue());

        final String query = "CALL apoc.import.parquet($file, $config)";
        Map<String, Object> params = Map.of("file", url, "config", MAPPING_ALL);
        testCall(db, query, params,
                r -> {
                    assertEquals(4L, r.get("nodes"));
                    assertEquals(1L, r.get("relationships"));
                });

        deleteFile(filename);
    }

    private String exportToParquetFile(String filename) {
        String file = db.executeTransactionally("CALL apoc.export.parquet.all($filename) YIELD file",
                Map.of("filename", filename),
                ParquetTestUtil::extractFileName);
        return file.replace("file://", "");
    }

    private void deleteFile(String filename) {
        File fileToDelete = new File(filename);
        if (fileToDelete.exists()) {
            boolean deleted = fileToDelete.delete();
            assertTrue(deleted);
        }
    }
}
