package apoc.export.csv;

import apoc.graph.Graphs;
import apoc.util.BinaryTestUtil;
import apoc.util.CompressionAlgo;
import apoc.util.HdfsTestUtils;
import apoc.util.TestUtil;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

import static apoc.ApocConfig.APOC_EXPORT_FILE_ENABLED;
import static apoc.ApocConfig.apocConfig;
import static apoc.util.CompressionAlgo.BLOCK_LZ4;
import static apoc.util.CompressionAlgo.NONE;
import static apoc.util.MapUtil.map;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

/**
 * @author mh
 * @since 22.05.16
 */
public class ExportCsvTest {
    private static final String EXPECTED = String.format("\"_id\",\"_labels\",\"age\",\"city\",\"kids\",\"male\",\"name\",\"street\",\"_start\",\"_end\",\"_type\"%n" +
            "\"0\",\":User:User1\",\"42\",\"\",\"[\"\"a\"\",\"\"b\"\",\"\"c\"\"]\",\"true\",\"foo\",\"\",,,%n" +
            "\"1\",\":User\",\"42\",\"\",\"\",\"\",\"bar\",\"\",,,%n" +
            "\"2\",\":User\",\"12\",\"\",\"\",\"\",\"\",\"\",,,%n" +
            "\"3\",\":Address:Address1\",\"\",\"Milano\",\"\",\"\",\"Andrea\",\"Via Garibaldi, 7\",,,%n" +
            "\"4\",\":Address\",\"\",\"\",\"\",\"\",\"Bar Sport\",\"\",,,%n" +
            "\"5\",\":Address\",\"\",\"\",\"\",\"\",\"\",\"via Benni\",,,%n" +
            ",,,,,,,,\"0\",\"1\",\"KNOWS\"%n" +
            ",,,,,,,,\"3\",\"4\",\"NEXT_DELIVERY\"%n");

    private static File directory = new File("target/import");
    static { //noinspection ResultOfMethodCallIgnored
        directory.mkdirs();
    }

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule()
            .withSetting(GraphDatabaseSettings.load_csv_file_url_root, directory.toPath().toAbsolutePath());

    private static MiniDFSCluster miniDFSCluster;

    @BeforeClass
    public static void setUp() throws Exception {
        TestUtil.registerProcedure(db, ExportCSV.class, Graphs.class);
        apocConfig().setProperty(APOC_EXPORT_FILE_ENABLED, true);
        db.executeTransactionally("CREATE (f:User1:User {name:'foo',age:42,male:true,kids:['a','b','c']})-[:KNOWS]->(b:User {name:'bar',age:42}),(c:User {age:12})");
        db.executeTransactionally("CREATE (f:Address1:Address {name:'Andrea', city: 'Milano', street:'Via Garibaldi, 7'})-[:NEXT_DELIVERY]->(a:Address {name: 'Bar Sport'}), (b:Address {street: 'via Benni'})");
        miniDFSCluster = HdfsTestUtils.getLocalHDFSCluster();
    }

    @AfterClass
    public static void tearDown() {
        if (miniDFSCluster!= null) {
            miniDFSCluster.shutdown();
        }
        db.shutdown();
    }

    @Test
    public void testExportAllCsvHDFS() throws Exception {
        assertHdfsFile(NONE);
    }

    @Test
    public void testExportAllCsvHDFSCompressed() {
        assertHdfsFile(BLOCK_LZ4);
    }

    private void assertHdfsFile(CompressionAlgo compression) {
        String fileExt = compression.equals(NONE) ? "" : ".lz4";
        String hdfsUrl = String.format("%s/user/%s/all.csv", miniDFSCluster.getURI().toString(), System.getProperty("user.name"));
        TestUtil.testCall(db, "CALL apoc.export.csv.all($file, $config)",
                map("file", hdfsUrl + fileExt, "config", map("compression", compression.name())),
                (r) -> {
                    try {
                        FileSystem fs = miniDFSCluster.getFileSystem();
                        FSDataInputStream inputStream = fs.open(new Path(String.format("/user/%s/all.csv" + fileExt, System.getProperty("user.name"))));
                        File output = Files.createTempFile("all", ".csv").toFile();
                        FileUtils.copyInputStreamToFile(inputStream, output);
                        assertEquals(6L, r.get("nodes"));
                        assertEquals(2L, r.get("relationships"));
                        assertEquals(12L, r.get("properties"));
                        assertEquals("database: nodes(6), rels(2)", r.get("source"));
                        assertEquals("csv", r.get("format"));
                        assertTrue("Should get time greater than 0",((long) r.get("time")) >= 0);
                        final String actual = BinaryTestUtil.readFileToString(output, StandardCharsets.UTF_8, compression);
                        assertEquals(EXPECTED, actual);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
    }

}
