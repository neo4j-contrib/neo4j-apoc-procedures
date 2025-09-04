package apoc.hadoop;

import apoc.util.Util;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.util.Map;

import static apoc.util.TestContainerUtil.testCall;

public class LoadHdfsBaseTest extends HdfsContainerBaseTest {

    @BeforeClass
    public static void setUp() throws Exception {
        HdfsContainerBaseTest.setUp();
        // copy csv file in hadoop
        namenode.execInContainer("hdfs", "dfs", "-mkdir", "/DATA/");
        namenode.execInContainer("bash", "-c", "echo \"a,b\n1,2\" > data.csv");
        namenode.execInContainer("hdfs", "dfs", "-put", "data.csv", "/DATA/");
    }
    
    @Test
    public void testApocLoadCsvFromHdfs() throws Exception {
        testCall(session, "CALL apoc.load.csv($url) YIELD map",
                Util.map("url", hdfsUrl + "/DATA/data.csv"),
                r -> {
                    Map<String, String> expected = Util.map("a", "1", "b", "2");
                    Assertions.assertEquals(expected, r.get("map"));
                });
    }
}
