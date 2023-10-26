package apoc.s3;

import apoc.export.parquet.ParquetTestUtil;
import apoc.util.s3.S3BaseTest;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.Map;

import static apoc.export.parquet.ParquetTest.MAPPING_ALL;
import static apoc.export.parquet.ParquetTestUtil.beforeClassCommon;
import static apoc.export.parquet.ParquetTestUtil.beforeCommon;
import static apoc.util.TestUtil.testResult;

public class ParquetS3Test extends S3BaseTest {

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
    }

}
