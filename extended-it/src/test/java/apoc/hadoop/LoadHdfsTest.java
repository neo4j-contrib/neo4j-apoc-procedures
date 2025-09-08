package apoc.hadoop;

import apoc.util.Util;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.testcontainers.containers.Container;

import java.util.Map;

import static apoc.util.TestContainerUtil.testCall;

public class LoadHdfsTest extends HdfsContainerBaseTest {

    @BeforeClass
    public static void setUp() throws Exception {
        HdfsContainerBaseTest.setUp();
        // copy csv file in hadoop
        Container.ExecResult execResult = namenode.execInContainer("hdfs", "dfs", "-mkdir", "/DATA/");
        System.out.println("execResult.getStdout() = " + execResult.getStdout());
        System.out.println("execResult.getStderr(() = " + execResult.getStderr());
        Container.ExecResult execResult1 = namenode.execInContainer("bash", "-c", "echo \"a,b\n1,2\" > data.csv");
        System.out.println("execResult1.getStdout() = " + execResult1.getStdout());
        System.out.println("execResult1.getStderr(() = " + execResult1.getStderr());
        Container.ExecResult execResult2 = namenode.execInContainer("hdfs", "dfs", "-put", "data.csv", "/DATA/");
        System.out.println("execResult2.getStdout() = " + execResult2.getStdout());
        System.out.println("execResult2.getStderr() = " + execResult2.getStderr());
    }
    
    @Test
    public void testApocLoadCsvFromHdfs() throws Exception {
//        Thread.sleep(3000);
        testCall(session, "CALL apoc.load.csv($url) YIELD map",
                Util.map("url", hdfsUrl + "/DATA/data.csv"),
                r -> {
                    Map<String, String> expected = Util.map("a", "1", "b", "2");
                    Assertions.assertEquals(expected, r.get("map"));
                });
    }
}
