package apoc.hadoop;

import apoc.util.collection.Iterators;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.Map;

import static apoc.export.parquet.ParquetTest.MAPPING_ALL;
import static apoc.export.parquet.ParquetTestUtil.assertNodeAndLabel;
import static apoc.export.parquet.ParquetUtil.FIELD_SOURCE_ID;
import static apoc.export.parquet.ParquetUtil.FIELD_TARGET_ID;
import static apoc.util.TestContainerUtil.testCall;
import static apoc.util.TestContainerUtil.testResult;
import static org.junit.Assert.*;

public class ParquetHdfsTest extends HdfsContainerBaseTest {

    private static final File directory = new File("target/hdfs-parquet-import");
    static { //noinspection ResultOfMethodCallIgnored
        directory.mkdirs();
    }

    @ClassRule
    public static TemporaryFolder hdfsDir = new TemporaryFolder();
    

    @Before
    public void before() {
        session.executeWrite(tx -> tx.run("MATCH (n) DETACH DELETE n").consume());

        session.executeWrite(tx -> tx.run("CREATE (f:User {name:'Adam',age:42,male:true,kids:['Sam','Anna','Grace', 'Qwe'], born:localdatetime('2015-05-18T19:32:24.000'), place:point({latitude: 13.1, longitude: 33.46789, height: 100.0})})-[:KNOWS {since: 1993, bffSince: duration('P5M1.5D')}]->(b:User {name:'Jim',age:42})").consume());
        session.executeWrite(tx -> tx.run("CREATE (:Another {foo:1, listDate: [date('1999'), date('2000')], listInt: [1,2]}), (:Another {bar:'Sam'})").consume());

    }

    @Test
    public void testFileRoundtripParquetAll() {

        String hdfsFileUrl = hdfsUrl + "/all.parquet";
        String file = session.executeWrite(tx -> tx.run("CALL apoc.export.parquet.all($url) YIELD file", Map.of("url", hdfsFileUrl)).single().get("file").asString());
        // check that file extracted from apoc.export is equals to `hdfs://path/to/file` url
        assertEquals(hdfsFileUrl, file);

        // check load procedure
        final String query = "CALL apoc.load.parquet($file, $config) YIELD value " +
                             "RETURN value";

        testResult(session, query, Map.of("file", file,  "config", MAPPING_ALL),
                value -> {
                    Map<String, Object> actual = value.next();
                    assertNodeAndLabel((Map) actual.get("value"), "User");
                    actual = value.next();
                    assertNodeAndLabel((Map) actual.get("value"), "User");
                    actual = value.next();
                    assertNodeAndLabel((Map) actual.get("value"), "Another");
                    actual = value.next();
                    assertNodeAndLabel((Map) actual.get("value"), "Another");
                    actual = value.next();
                    Map relMap = (Map) actual.get("value");
                    assertTrue(relMap.get(FIELD_SOURCE_ID) instanceof Long);
                    assertTrue(relMap.get(FIELD_TARGET_ID) instanceof Long);
                    assertFalse(value.hasNext());
                });

        // check import procedure
        Map<String, Object> params = Map.of("file", file, "config", MAPPING_ALL);
        // remove current data
        session.executeWrite(tx -> tx.run("MATCH (n) DETACH DELETE n").consume());

        final String queryImport = "CALL apoc.import.parquet($file, $config)";
        testCall(session, queryImport, params,
                r -> {
                    assertEquals(4L, r.get("nodes"));
                    assertEquals(1L, r.get("relationships"));
                });

        testResult(session, "MATCH (start:User)-[rel:KNOWS]->(end:User) RETURN start, rel, end", r -> {
            long count = Iterators.count(r);
            assertEquals(1, count);
        });

        testResult(session, "MATCH (m:Another) RETURN m", r -> {
            long count = Iterators.count(r);
            assertEquals(2, count);
        });

    }
}
