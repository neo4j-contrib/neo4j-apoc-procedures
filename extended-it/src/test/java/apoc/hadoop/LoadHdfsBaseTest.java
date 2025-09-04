package apoc.hadoop;

import apoc.util.Util;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.*;

import org.junit.jupiter.api.Assertions;

import java.util.Map;

import static apoc.util.TestContainerUtil.testCall;
import static apoc.util.TestUtil.testResult;
import static org.junit.Assert.assertEquals;

//@Ignore("It fails due to `java.lang.NoClassDefFoundError: org/eclipse/jetty/servlet`")
public class LoadHdfsBaseTest extends HdfsContainerBaseTest {

//    @ClassRule
//    public static TemporaryFolder hdfsDir = new TemporaryFolder();
//
//    @Rule
//    public DbmsRule db = new ImpermanentDbmsRule();

    private MiniDFSCluster miniDFSCluster;

//    @Before public void setUp() throws Exception {
//        TestUtil.registerProcedure(db, LoadCsv.class);
//        apocConfig().setProperty(APOC_IMPORT_FILE_ENABLED, true);
//        miniDFSCluster = HdfsTestUtils.getLocalHDFSCluster(hdfsDir.getRoot());
//		FileSystem fs = miniDFSCluster.getFileSystem();
//		String fileName = "test.csv";
//		Path file = new Path(fileName);
//		try (OutputStream out = fs.create(file);) {
//			URL url = ClassLoader.getSystemResource(fileName);
//			try (InputStream in = url.openStream();) {
//				IOUtils.copy(in, out);
//			}
//		}
//    }

    @BeforeClass
    public static void setUp() throws Exception {
        HdfsContainerBaseTest.setUp();
        // Esegui il comando di copia dopo l'avvio del container
//        Container.ExecResult execResult0 = hdfsCluster.getContainerByServiceName(NAMENODE_1).get()
        namenode.execInContainer("hdfs", "dfs", "-mkdir", "/DATA/");
        namenode.execInContainer("bash", "-c", "echo \"a,b\n1,2\" > data.csv");
        namenode.execInContainer("hdfs", "dfs", "-put", "data.csv", "/DATA/");
    }

//    @After public void tearDown() {
//        miniDFSCluster.shutdown();
//    }

    @Test
    public void testApocLoadCsvFromHdfs() throws Exception {
        testCall(session, "CALL apoc.load.csv($url) YIELD map",
                Util.map("url", hdfsUrl + "/DATA/data.csv"),
                r -> {
                    Map<String, String> expected = Util.map("a", "1", "b", "2");
                    Assertions.assertEquals(expected, r.get("map"));
                });
    }

//    @Test public void testLoadCsvFromHDFS() throws Exception {
//        testResult(db, "CALL apoc.load.csv($url,{results:['map','list','stringMap','strings']})", map("url", String.format("%s/user/%s/%s",
//        		miniDFSCluster.getURI().toString(),
//        		System.getProperty("user.name"), "test.csv")), // 'hdfs://localhost:12345/user/<sys_user_name>/test.csv'
//                (r) -> {
//                    LoadCsvTest.assertRow(r,0L,"name","Selma","age","8");
//                    LoadCsvTest.assertRow(r,1L,"name","Rana","age","11");
//                    LoadCsvTest.assertRow(r,2L,"name","Selina","age","18");
//                    assertEquals(false, r.hasNext());
//                });
//    }
}
