package apoc.load.relative;

import apoc.load.LoadCsv;
import apoc.load.LoadJson;
import apoc.load.Xml;
import apoc.util.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mortbay.util.StringMap;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static apoc.load.relative.LoadXmlResult.StringXmlNestedMap;
import static apoc.load.relative.LoadXmlResult.StringXmlNestedSimpleMap;
import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testResult;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class LoadRelativePathTest {

    private GraphDatabaseService db;
    private static String PATH = new File(LoadRelativePathTest.class.getClassLoader().getResource("test.csv").getPath()).getParent();

    @Before public void setUp() throws Exception {
        db = new TestGraphDatabaseFactory()
                .newImpermanentDatabaseBuilder()
                .setConfig("apoc.import.file.enabled","true")
                .setConfig("dbms.directories.import", PATH)
                .setConfig("dbms.security.allow_csv_import_from_file_urls","true")
                .newGraphDatabase();
        TestUtil.registerProcedure(db, LoadCsv.class);
        TestUtil.registerProcedure(db, LoadJson.class);
        TestUtil.registerProcedure(db, Xml.class);
    }

    @After public void tearDown() {
        db.shutdown();
    }

    //CSV
    @Test public void testLoadRelativePathCsv() {
        String url = "test.csv";
        testResult(db, "CALL apoc.load.csv({url},{results:['map','list','stringMap','strings']})", map("url",url), // 'file:test.csv'
                (r) -> {
                    assertRow(r,0L,"name","Selma","age","8");
                    assertRow(r,1L,"name","Rana","age","11");
                    assertRow(r,2L,"name","Selina","age","18");
                    assertFalse(r.hasNext());
                });
    }

    private static void assertRow(Result r, long lineNo, Object...data) {
        Map<String, Object> row = r.next();
        Map<String, Object> map = map(data);
        List<Object> values = new ArrayList<>(map.values());
        StringMap stringMap = new StringMap();
        stringMap.putAll(map);
        assertEquals(map, row.get("map"));
        assertEquals(values, row.get("list"));
        assertEquals(values, row.get("strings"));
        assertEquals(stringMap, row.get("stringMap"));
        assertEquals(lineNo, row.get("lineNo"));
    }

    //JSON
    @Test public void testLoadRelativePathJson() {
        String url = "file:/map.json";
        testCall(db, "CALL apoc.load.json({url})",map("url",url), // 'file:map.json' YIELD value RETURN value
                (row) -> assertEquals(map("foo",asList(1L,2L,3L)), row.get("value")));
    }

    //XML
    @Test
    public void testLoadRelativePathXml() {
        testCall(db, "CALL apoc.load.xml('file:///xml/databases.xml')", //  YIELD value RETURN value
                (row) -> {
                    Object value = row.get("value");
                    assertEquals(StringXmlNestedMap(), value);
                });
    }

    @Test
    public void testLoadRelativePathSimple() {
        testCall(db, "CALL apoc.load.xmlSimple('/xml/databases.xml')", //  YIELD value RETURN value
                (row) -> {
                    Object value = row.get("value");
                    assertEquals(StringXmlNestedSimpleMap(), value);
                });
    }

}