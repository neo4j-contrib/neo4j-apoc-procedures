package apoc.hadoop;

import apoc.util.*;
import org.junit.*;
import org.junit.rules.TemporaryFolder;
import org.neo4j.driver.Value;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static apoc.util.CompressionAlgo.BLOCK_LZ4;
import static apoc.util.CompressionAlgo.NONE;
import static apoc.util.MapUtil.map;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

public class ExportCsvHdfsTest extends HdfsContainerBaseTest {
    private static final String EXPECTED = String.format("0,:User:User1,42,,[\"a\",\"b\",\"c\"],true,foo,,,,%n" +
            "1,:User,42,,,,bar,,,,%n" +
            "2,:User,12,,,,,,,,%n" +
            "3,:Address:Address1,,Milano,,,Andrea,Via Garibaldi, 7,,,%n" +
            "4,:Address,,,,,Bar Sport,,,,%n" +
            "5,:Address,,,,,,via Benni,,,%n" +
            ",,,,,,,,0,1,KNOWS%n" +
            ",,,,,,,,3,4,NEXT_DELIVERY");

    @ClassRule
    public static TemporaryFolder storeDir = new TemporaryFolder();
    
    @ClassRule
    public static TemporaryFolder hdfsDir = new TemporaryFolder();

    @BeforeClass
    public static void setUp() throws Exception {
        HdfsContainerBaseTest.setUp();
        
        session.executeWrite(tx -> tx.run("CREATE (f:User1:User {name:'foo',age:42,male:true,kids:['a','b','c']})-[:KNOWS]->(b:User {name:'bar',age:42}),(c:User {age:12})").consume());
        session.executeWrite(tx -> tx.run("CREATE (f:Address1:Address {name:'Andrea', city: 'Milano', street:'Via Garibaldi, 7'})-[:NEXT_DELIVERY]->(a:Address {name: 'Bar Sport'}), (b:Address {street: 'via Benni'})").consume());
    }

    @Test
    public void testExportAllCsvHDFS() {
        String url = assertHdfsFile(NONE);

        String actual = session.executeRead(tx -> tx.run(
                                "CALL apoc.load.csv($url, $config) YIELD strings",
                                map("url", url,
                                        "config", map("results", List.of("strings")))
                        ).stream()
                        .map(i -> String.join(",", i.get("strings").asList(Value::asString)))
                        .collect(Collectors.joining("\n"))
        );

        assertEquals(EXPECTED, actual);
    }

    @Test
    public void testExportAllCsvHDFSCompressed() {
        assertHdfsFile(BLOCK_LZ4);
    }

    private String assertHdfsFile(CompressionAlgo compression) {
        String fileExt = compression.equals(NONE) ? "" : ".lz4";

        AtomicReference<String> url = new AtomicReference<>();
        TestContainerUtil.testCall(session, "CALL apoc.export.csv.all($file, $config)",
                map("file", hdfsUrl + "/all.csv" + fileExt, "config", map("compression", compression.name())),
                (r) -> {
                    try {
                        assertEquals(6L, r.get("nodes"));
                        assertEquals(2L, r.get("relationships"));
                        assertEquals(12L, r.get("properties"));
                        assertEquals("database: nodes(6), rels(2)", r.get("source"));
                        assertEquals("csv", r.get("format"));
                        url.set( (String) r.get("file") );
                        assertTrue("Should get time greater than 0",((long) r.get("time")) >= 0);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
        
        return url.get();
    }

}
