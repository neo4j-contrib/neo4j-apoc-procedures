package apoc.export.parquet;

import apoc.util.HdfsTestUtils;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.io.File;
import java.util.Map;

import static apoc.export.parquet.ParquetTest.MAPPING_ALL;
import static apoc.export.parquet.ParquetTestUtil.beforeClassCommon;
import static apoc.export.parquet.ParquetTestUtil.beforeCommon;
import static apoc.export.parquet.ParquetTestUtil.testImportAllCommon;
import static apoc.util.TestUtil.testResult;
import static org.junit.Assert.assertEquals;

public class ParquetHdfsTest {

    private static final File directory = new File("target/hdfs-parquet-import");
    static { //noinspection ResultOfMethodCallIgnored
        directory.mkdirs();
    }

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule()
            .withSetting(GraphDatabaseSettings.load_csv_file_url_root, directory.toPath().toAbsolutePath());

    private static MiniDFSCluster miniDFSCluster;

    @BeforeClass
    public static void setUp() throws Exception {
        beforeClassCommon(db);
        miniDFSCluster = HdfsTestUtils.getLocalHDFSCluster();
    }

    @Before
    public void before() {
        beforeCommon(db);
    }

    @AfterClass
    public static void tearDown() {
        if (miniDFSCluster!= null) {
            miniDFSCluster.shutdown();
        }
        db.shutdown();
    }

    @Test
    public void testFileRoundtripParquetAll() {
        String hdfsUrl = String.format("%s/user/%s/all.parquet", miniDFSCluster.getURI().toString(), System.getProperty("user.name"));

        // check export procedure
        String file = db.executeTransactionally("CALL apoc.export.parquet.all($url) YIELD file",
                Map.of("url", hdfsUrl),
                ParquetTestUtil::extractFileName);

        // check that file extracted from apoc.export is equals to `hdfs://path/to/file` url
        assertEquals(hdfsUrl, file);

        // check load procedure
        final String query = "CALL apoc.load.parquet($file, $config) YIELD value " +
                             "RETURN value";

        testResult(db, query, Map.of("file", file,  "config", MAPPING_ALL),
                ParquetTestUtil::roundtripLoadAllAssertions);

        // check import procedure
        Map<String, Object> params = Map.of("file", file, "config", MAPPING_ALL);
        testImportAllCommon(db, params);

    }
}
