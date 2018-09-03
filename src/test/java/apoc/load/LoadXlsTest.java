package apoc.load;

import apoc.util.TestUtil;
import org.junit.*;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Result;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testResult;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

public class LoadXlsTest {

    private GraphDatabaseService db;

    @Before public void setUp() throws Exception {
        db = new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder().setConfig("apoc.import.file.enabled","true").newGraphDatabase();
        TestUtil.registerProcedure(db, LoadXls.class);
    }

    @After public void tearDown() {
        db.shutdown();
    }

    @Test public void testLoadXls() throws Exception {
        URL url = ClassLoader.getSystemResource("load_test.xlsx");
        testResult(db, "CALL apoc.load.xls({url},'Full',{mapping:{Integer:{type:'int'}, Array:{type:'int',array:true,arraySep:';'}}})", map("url",url.toString()), // 'file:load_test.xlsx'
                (r) -> {
                    assertRow(r,0L,"String","Test","Boolean",true,"Integer",2L,"Float",1.5d,"Array",asList(1L,2L,3L));
                    assertEquals(false, r.hasNext());
                });
    }
    @Test public void testLoadBrokenHeader() throws Exception {
        URL url = ClassLoader.getSystemResource("brokenHeader.xls");
        BiFunction<String,Boolean,Long> query = (sheet,header) -> db.execute("CALL apoc.load.xls({url},{sheet},{header:{header}}) yield map return count(*) as c", map("header",header,"sheet",sheet,"url", url.toString())).<Long>columnAs("c").next();
        try {
            query.apply("temp",true);
            fail("Should fail with Header Error");
        } catch(QueryExecutionException qee) {
            assertEquals("Failed to invoke procedure `apoc.load.xls`: Caused by: java.lang.IllegalStateException: Header at position 4 doesn't have a value",
                    qee.getMessage());
        }
        try {
            query.apply("foo",false);
            fail("Should fail with Sheet Error");
        } catch(QueryExecutionException qee) {
            assertEquals("Failed to invoke procedure `apoc.load.xls`: Caused by: java.lang.IllegalStateException: Sheet foo not found",
                    qee.getMessage());
        }

        assertEquals(2L, (long)query.apply("temp",false));
    }
    @Test public void testLoadXlsMany() throws Exception {
        URL url = ClassLoader.getSystemResource("load_test.xlsx");
        testResult(db, "CALL apoc.load.xls({url},'Many',{mapping:{Float:{type:'float'}, Array:{type:'int',array:true,arraySep:';'}}})", map("url",url.toString()), // 'file:load_test.xlsx'
                (r) -> {
                    assertRow(r,0L,"String","A","Boolean",true ,"Integer",0L,"Float",0d,"Array", emptyList());
                    assertRow(r,1L,"String","B","Boolean",false,"Integer",1L,"Float",0.5d,"Array", asList(1L));
                    assertRow(r,2L,"String","C","Boolean",true ,"Integer",2L,"Float",1.0d,"Array", asList(1L,2L));
                    assertRow(r,3L,"String","D","Boolean",false,"Integer",3L,"Float",1.5d,"Array", asList(1L,2L,3L));
                    assertRow(r,4L,"String","E","Boolean",true ,"Integer",4L,"Float",2.0d,"Array", asList(1L,2L,3L,4L));
                    assertEquals(false, r.hasNext());
                });
    }

    @Test public void testLoadXlsOffset() throws Exception {
        URL url = ClassLoader.getSystemResource("load_test.xlsx");
        testResult(db, "CALL apoc.load.xls({url},'Offset!B2:F3',{mapping:{Integer:{type:'int'}, Array:{type:'int',array:true,arraySep:';'}}})", map("url",url.toString()), // 'file:load_test.xlsx'
                (r) -> {
                    assertRow(r,0L,"String","Test","Boolean",true,"Integer",2L,"Float",1.5d,"Array",asList(1L,2L,3L));
                    assertEquals(false, r.hasNext());
                });
    }

    @Test public void testLoadXlsNoHeaders() throws Exception {
        URL url = ClassLoader.getSystemResource("load_test.xlsx");
        testCall(db, "CALL apoc.load.xls({url},'NoHeader',{header:false})", map("url",url.toString()), // 'file:load_test.xlsx'
                (r) -> {
                    assertEquals(0L,r.get("lineNo"));
                    assertEquals(asList("Test",true,2L,1.5d,"1;2;3"),r.get("list"));
                });
    }

    /*
    WITH 'file:///load_test.xlsx' AS url
CALL apoc.load.xls(url,) YIELD map AS m
RETURN m.col_1,m.col_2,m.col_3
     */
    @Test public void testLoadCsvWithEmptyColumns() throws Exception {
        URL url = ClassLoader.getSystemResource("load_test.xlsx");
        testResult(db, "CALL apoc.load.xls({url},'Empty',{failOnError:false,mapping:{col_2:{type:'int'}}})", map("url",url.toString()), // 'file:load_test.xlsx'
                (r) -> {
                    Map<String, Object> row = r.next();
                    assertEquals(map("col_1", 1L,"col_2", null,"col_3", 1L), row.get("map"));
                    row = r.next();
                    assertEquals(map("col_1", 2L,"col_2", 2L,"col_3", ""), row.get("map"));
                    row = r.next();
                    assertEquals(map("col_1", 3L,"col_2", 3L,"col_3", 3L), row.get("map"));
                    assertEquals(false, r.hasNext());
                });
        testResult(db, "CALL apoc.load.xls({url},'Empty',{failOnError:false,nullValues:[''], mapping:{col_1:{type:'int'}}})", map("url",url.toString()), // 'file:load_test.xlsx'
                (r) -> {
                    Map<String, Object> row = r.next();
                    assertEquals(map("col_1", 1L,"col_2", null,"col_3", 1L), row.get("map"));
                    row = r.next();
                    assertEquals(map("col_1", 2L,"col_2", 2L,"col_3", null), row.get("map"));
                    row = r.next();
                    assertEquals(map("col_1", 3L,"col_2", 3L,"col_3", 3L), row.get("map"));
                    assertEquals(false, r.hasNext());
                });
        testResult(db, "CALL apoc.load.xls({url},'Empty',{failOnError:false,mapping:{col_3:{type:'int',nullValues:['']}}})", map("url",url.toString()), // 'file:load_test.xlsx'
                (r) -> {
                    Map<String, Object> row = r.next();
                    assertEquals(map("col_1", 1L,"col_2", "","col_3", 1L), row.get("map"));
                    row = r.next();
                    assertEquals(map("col_1", 2L,"col_2", 2L,"col_3", null), row.get("map"));
                    row = r.next();
                    assertEquals(map("col_1", 3L,"col_2", 3L,"col_3", 3L), row.get("map"));
                    assertEquals(false, r.hasNext());
                });
    }

    static void assertRow(Result r, long lineNo, Object...data) {
        Map<String, Object> row = r.next();
        Map<String, Object> map = map(data);
        assertEquals(map, row.get("map"));
        Map<Object, Object> stringMap = new LinkedHashMap<>(map.size());
        map.forEach((k,v) -> stringMap.put(k,v == null ? null : v.toString()));
        assertEquals(new ArrayList<>(map.values()), row.get("list"));
        assertEquals(lineNo, row.get("lineNo"));
    }
    static void assertRow(Result r, String name, Number age, long lineNo) {
        Map<String, Object> row = r.next();
        assertEquals(map("name", name,"age", age), row.get("map"));
        assertEquals(asList(name, age), row.get("list"));
        assertEquals(lineNo, row.get("lineNo"));
    }

    @Test public void testLoadCsvSkip() throws Exception {
        URL url = ClassLoader.getSystemResource("load_test.xlsx");
        testResult(db, "CALL apoc.load.xls({url},'Kids',{skip:1,limit:1})", map("url",url.toString()), // 'file:load_test.xlsx'
                (r) -> {
                    assertRow(r, "Rana", 11L, 0L);
                    assertEquals(false, r.hasNext());
                });
    }
    @Test public void testLoadCsvIgnoreFields() throws Exception {
        URL url = ClassLoader.getSystemResource("load_test.xlsx");
        testResult(db, "CALL apoc.load.xls({url},'Kids',{ignore:['age']})", map("url",url.toString()), // 'file:load_test.xlsx'
                (r) -> {
                    assertRow(r,0L,"name","Selma");
                    assertRow(r,1L,"name","Rana");
                    assertRow(r,2L,"name","Selina");
                    assertEquals(false, r.hasNext());
                });
    }

    @Test
    @Ignore
    public void testLoadCsvByUrl() throws Exception {
        URL url = new URL("https://raw.githubusercontent.com/neo4j-contrib/neo4j-apoc-procedures/3.3/src/test/resources/load_test.xlsx");
        testResult(db, "CALL apoc.load.xls({url},'Kids')", map("url", url.toString()),
                (r) -> {
                    assertRow(r,0L,"name","Selma","age","8");
                    assertRow(r,1L,"name","Rana","age","11");
                    assertRow(r,2L,"name","Selina","age","18");
                    assertEquals(false, r.hasNext());
                });

    }

    @Test
    @Ignore
    public void testLoadCsvByUrlRedirect() throws Exception {
        URL url = new URL("http://bit.ly/2nXgHA2");
        testResult(db, "CALL apoc.load.xls({url},'Kids')", map("url", url.toString()),
                (r) -> {
                    assertRow(r,0L,"name","Selma","age","8");
                    assertRow(r,1L,"name","Rana","age","11");
                    assertRow(r,2L,"name","Selina","age","18");
                    assertEquals(false, r.hasNext());
                });
    }

    @Test
    public void testLoadCsvNoFailOnError() throws Exception {
        String url = "load_test.xlsx";
        testResult(db, "CALL apoc.load.xls({url},'Kids',{failOnError:false})", map("url",url), // 'file:load_test.xlsx'
                (r) -> {
                    Map<String, Object> row = r.next();
                    assertEquals(0L, row.get("lineNo"));
                    assertEquals(emptyList(), row.get("list"));
                    assertEquals(Collections.emptyMap(), row.get("map"));
                    assertEquals(false, r.hasNext());
                });
    }
}
