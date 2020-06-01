package apoc.load;

import apoc.ApocSettings;
import apoc.util.TestUtil;
import apoc.util.Util;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Result;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.net.URL;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testResult;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.junit.Assert.*;

public class LoadXlsTest {

    private static String loadTest = Thread.currentThread().getContextClassLoader().getResource("load_test.xlsx").getPath();
    private static String testDate = Thread.currentThread().getContextClassLoader().getResource("test_date.xlsx").getPath();
    private static String brokenHeader = Thread.currentThread().getContextClassLoader().getResource("brokenHeader.xls").getPath();
    private static String testColumnsAfterZ = Thread.currentThread().getContextClassLoader().getResource("testLoadXlsColumnsAfterZ.xlsx").getPath();

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule()
            .withSetting(ApocSettings.apoc_import_file_enabled, true);

    @Before public void setUp() throws Exception {
        TestUtil.registerProcedure(db, LoadXls.class);
    }

    @Test public void testLoadXls() throws Exception {
        testResult(db, "CALL apoc.load.xls($url,'Full',{mapping:{Integer:{type:'int'}, Array:{type:'int',array:true,arraySep:';'}}})", map("url",loadTest), // 'file:load_test.xlsx'
                (r) -> {
                    assertRow(r,0L,"String","Test","Boolean",true,"Integer",2L,"Float",1.5d,"Array",asList(1L,2L,3L));
                    assertFalse("Should not have another row",r.hasNext());
                });
    }
    @Test public void testLoadBrokenHeader() throws Exception {
        BiFunction<String,Boolean,Long> query = (sheet,header) -> db.executeTransactionally(
                "CALL apoc.load.xls($url,$sheet,{header:$header}) yield map return count(*) as c",
                map("header",header,"sheet",sheet,"url", brokenHeader ),
                result -> Iterators.single(result.columnAs("c")));

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
        testResult(db, "CALL apoc.load.xls($url,'Many',{mapping:{Float:{type:'float'}, Array:{type:'int',array:true,arraySep:';'}}})", map("url",loadTest), // 'file:load_test.xlsx'
                (r) -> {
                    assertRow(r,0L,"String","A","Boolean",true ,"Integer",0L,"Float",0d,"Array", emptyList());
                    assertRow(r,1L,"String","B","Boolean",false,"Integer",1L,"Float",0.5d,"Array", asList(1L));
                    assertRow(r,2L,"String","C","Boolean",true ,"Integer",2L,"Float",1.0d,"Array", asList(1L,2L));
                    assertRow(r,3L,"String","D","Boolean",false,"Integer",3L,"Float",1.5d,"Array", asList(1L,2L,3L));
                    assertRow(r,4L,"String","E","Boolean",true ,"Integer",4L,"Float",2.0d,"Array", asList(1L,2L,3L,4L));
                    assertFalse("Should not have another row",r.hasNext());
                });
    }

    @Test public void testLoadXlsOffset() throws Exception {
        testResult(db, "CALL apoc.load.xls($url,'Offset!B2:F3',{mapping:{Integer:{type:'int'}, Array:{type:'int',array:true,arraySep:';'}}})", map("url",loadTest), // 'file:load_test.xlsx'
                (r) -> {
                    assertRow(r,0L,"String","Test","Boolean",true,"Integer",2L,"Float",1.5d,"Array",asList(1L,2L,3L));
                    assertFalse(r.hasNext());
                });
    }

    @Test public void testLoadXlsNoHeaders() throws Exception {
        testCall(db, "CALL apoc.load.xls($url,'NoHeader',{header:false})", map("url",loadTest), // 'file:load_test.xlsx'
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
        testResult(db, "CALL apoc.load.xls($url,'Empty',{failOnError:false,mapping:{col_2:{type:'int'}}})", map("url",loadTest), // 'file:load_test.xlsx'
                (r) -> {
                    Map<String, Object> row = r.next();
                    assertEquals(map("col_1", 1L,"col_2", null,"col_3", 1L), row.get("map"));
                    row = r.next();
                    assertEquals(map("col_1", 2L,"col_2", 2L,"col_3", ""), row.get("map"));
                    row = r.next();
                    assertEquals(map("col_1", 3L,"col_2", 3L,"col_3", 3L), row.get("map"));
                    assertFalse("Should not have another row",r.hasNext());
                });
        testResult(db, "CALL apoc.load.xls($url,'Empty',{failOnError:false,nullValues:[''], mapping:{col_1:{type:'int'}}})", map("url",loadTest), // 'file:load_test.xlsx'
                (r) -> {
                    Map<String, Object> row = r.next();
                    assertEquals(map("col_1", 1L,"col_2", null,"col_3", 1L), row.get("map"));
                    row = r.next();
                    assertEquals(map("col_1", 2L,"col_2", 2L,"col_3", null), row.get("map"));
                    row = r.next();
                    assertEquals(map("col_1", 3L,"col_2", 3L,"col_3", 3L), row.get("map"));
                    assertFalse("Should not have another row",r.hasNext());
                });
        testResult(db, "CALL apoc.load.xls($url,'Empty',{failOnError:false,mapping:{col_3:{type:'int',nullValues:['']}}})", map("url",loadTest), // 'file:load_test.xlsx'
                (r) -> {
                    Map<String, Object> row = r.next();
                    assertEquals(map("col_1", 1L,"col_2", "","col_3", 1L), row.get("map"));
                    row = r.next();
                    assertEquals(map("col_1", 2L,"col_2", 2L,"col_3", null), row.get("map"));
                    row = r.next();
                    assertEquals(map("col_1", 3L,"col_2", 3L,"col_3", 3L), row.get("map"));
                    assertFalse("Should not have another row",r.hasNext());
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
        testResult(db, "CALL apoc.load.xls($url,'Kids',{skip:1,limit:1})", map("url",loadTest), // 'file:load_test.xlsx'
                (r) -> {
                    assertRow(r, "Rana", 11L, 0L);
                    assertFalse("Should not have another row",r.hasNext());
                });
    }
    @Test public void testLoadCsvIgnoreFields() throws Exception {
        testResult(db, "CALL apoc.load.xls($url,'Kids',{ignore:['age']})", map("url",loadTest), // 'file:load_test.xlsx'
                (r) -> {
                    assertRow(r,0L,"name","Selma");
                    assertRow(r,1L,"name","Rana");
                    assertRow(r,2L,"name","Selina");
                    assertFalse("Should not have another row",r.hasNext());
                });
    }

    @Test
    @Ignore
    public void testLoadCsvByUrl() throws Exception {
        URL url = new URL("https://raw.githubusercontent.com/neo4j-contrib/neo4j-apoc-procedures/3.3/src/test/resources/load_test.xlsx");
        testResult(db, "CALL apoc.load.xls($url,'Kids')", map("url", url.toString()),
                (r) -> {
                    assertRow(r,0L,"name","Selma","age","8");
                    assertRow(r,1L,"name","Rana","age","11");
                    assertRow(r,2L,"name","Selina","age","18");
                    assertFalse(r.hasNext());
                });

    }

    @Test
    @Ignore
    public void testLoadCsvByUrlRedirect() throws Exception {
        URL url = new URL("http://bit.ly/2nXgHA2");
        testResult(db, "CALL apoc.load.xls($url,'Kids')", map("url", url.toString()),
                (r) -> {
                    assertRow(r,0L,"name","Selma","age","8");
                    assertRow(r,1L,"name","Rana","age","11");
                    assertRow(r,2L,"name","Selina","age","18");
                    assertFalse("Should not have another row",r.hasNext());
                });
    }

    @Test
    public void testLoadCsvNoFailOnError() throws Exception {
        testResult(db, "CALL apoc.load.xls($url,'Kids',{failOnError:false})", map("url",loadTest), // 'file:load_test.xlsx'
                (r) -> {
                    assertRow(r,0L,"name","Selma","age",8L);
                    assertRow(r,1L,"name","Rana","age",11L);
                    assertRow(r,2L,"name","Selina","age",18L);
                    assertFalse("Should not have another row",r.hasNext());
                });
    }

    @Test
    public void testLoadXlsDateWithMappingTypeString() throws Exception {

        LocalDateTime date = LocalDateTime.of(2018,10,10, 0, 0, 0);
        LocalDateTime time = LocalDateTime.of(1899,12,31, 12,01,10);

            testResult(db, "CALL apoc.load.xls($url,'sheet',{mapping:{Date:{type:'String', dateFormat: 'iso_date'}}})", map("url", testDate),
                (r) -> {
                    Map<String, Object> row = r.next();
                    assertEquals(0L, row.get("lineNo"));
                    assertEquals(asList("2018/05/10", "2018/10/05", "Alan"), row.get("list"));
                    assertEquals(Util.map("Date", "2018/05/10", "Data", "2018/10/05","Name", "Alan"), row.get("map"));
                    assertTrue( r.hasNext());
                    row = r.next();
                    assertEquals(1L, row.get("lineNo"));
                    assertEquals(asList("2018-09-10", date, "Jack"), row.get("list"));
                    assertEquals(Util.map("Date", "2018-09-10", "Data",  date,  "Name", "Jack"), row.get("map"));
                    assertTrue("Should have another row",r.hasNext());
                    row = r.next();
                    assertEquals(2L, row.get("lineNo"));
                    assertEquals(asList("2018/05/10 12:10:10", date, date), row.get("list"));
                    assertEquals(Util.map("Date", "2018/05/10 12:10:10", "Data", date, "Name", date), row.get("map"));
                    assertTrue("Should have another row",r.hasNext());
                    row = r.next();
                    assertEquals(3L, row.get("lineNo"));
                    assertEquals(asList(null, date, time), row.get("list"));
                    assertEquals(Util.map("Date", null, "Data", date, "Name", time), row.get("map"));
                    assertTrue("Should have another row",r.hasNext());
                    row = r.next();
                    assertEquals(4L, row.get("lineNo"));
                    assertEquals(asList("2011-01-01T12:00:00.05381+01:00", null, null), row.get("list"));
                    assertEquals(Util.map("Date", "2011-01-01T12:00:00.05381+01:00", "Data", null, "Name", null), row.get("map"));
                    assertFalse("Should not have another row",r.hasNext());
                });
    }

    @Test
    public void testLoadXlsDateWithMappingArrayTypeString() throws Exception {

        LocalDateTime date = LocalDateTime.of(2018,10,10, 0, 0, 0);
        LocalDateTime time = LocalDateTime.of(1899,12,31, 12,01,10);
        String elementExpected = "2018-09-10T00:00:00";

        testResult(db, "CALL apoc.load.xls($url,'sheet',{mapping:{Date:{type:'String', dateFormat: '', dateParse: []}}})", map("url",testDate),
                (r) -> {
                    Map<String, Object> row = r.next();
                    assertEquals(0L, row.get("lineNo"));
                    assertEquals(asList("2018/05/10", "2018/10/05", "Alan"), row.get("list"));
                    assertEquals(Util.map("Date", "2018/05/10", "Data", "2018/10/05", "Name", "Alan"), row.get("map"));
                    assertTrue("Should have another row", r.hasNext());
                    row = r.next();
                    assertEquals(1L, row.get("lineNo"));
                    assertEquals(asList(elementExpected, date, "Jack"), row.get("list"));
                    assertEquals(Util.map("Date", elementExpected, "Data",  date,  "Name", "Jack"), row.get("map"));
                    assertTrue("Should have another row",r.hasNext());
                    row = r.next();
                    assertEquals(2L, row.get("lineNo"));
                    assertEquals(asList("2018/05/10 12:10:10", date, date), row.get("list"));
                    assertEquals(Util.map("Date", "2018/05/10 12:10:10", "Data", date, "Name", date), row.get("map"));
                    assertTrue(r.hasNext());
                    row = r.next();
                    assertEquals(3L, row.get("lineNo"));
                    assertEquals(asList(null, date, time), row.get("list"));
                    assertEquals(Util.map("Date", null, "Data", date, "Name", time), row.get("map"));
                    assertTrue("Should have another row",r.hasNext());
                    row = r.next();
                    assertEquals(4L, row.get("lineNo"));
                    assertEquals(asList("2011-01-01T12:00:00.05381+01:00", null, null), row.get("list"));
                    assertEquals(Util.map("Date", "2011-01-01T12:00:00.05381+01:00", "Data", null, "Name", null), row.get("map"));
                    assertFalse("Should not have another row",r.hasNext());
                });
    }

    @Test
    public void testLoadXlsDateWithMappingArrayTypeDate() throws Exception {
        LocalDateTime time = LocalDateTime.of(1899,12,31, 12,01,10);
        LocalDateTime localDateTimeValue = LocalDateTime.of(2018,9,10, 0,0,0);
        LocalDateTime localDateTimeValue1 = LocalDateTime.of(2018,10,10, 0,0,0);

        LocalDate localDate = LocalDate.of(2018,10,5);
        LocalDate LocalDate1 = LocalDate.of(2018,10,10);

        List pattern = asList("wrongPath", "dd-MM-yyyy", "dd/MM/yyyy", "yyyy/MM/dd", "yyyy/dd/MM", "yyyy-dd-MM'T'hh:mm:ss");

        testResult(db, "CALL apoc.load.xls($url,'sheet',{mapping:{Data:{type: 'Date', dateParse: $pattern}}})", map("url",testDate, "pattern", pattern),
                (r) -> {
                    Map<String, Object> row = r.next();
                    assertEquals(0L, row.get("lineNo"));
                    assertEquals(asList("2018/05/10", localDate, "Alan"), row.get("list"));
                    assertEquals(Util.map("Date", "2018/05/10", "Data", localDate, "Name", "Alan"), row.get("map"));
                    assertTrue("Should have another row", r.hasNext());
                    row = r.next();
                    assertEquals(1L, row.get("lineNo"));
                    assertEquals(asList(localDateTimeValue, LocalDate1, "Jack"), row.get("list"));
                    assertEquals(Util.map("Date", localDateTimeValue, "Data",  LocalDate1,  "Name", "Jack"), row.get("map"));
                    assertTrue("Should have another row",r.hasNext());
                    row = r.next();
                    assertEquals(2L, row.get("lineNo"));
                    assertEquals(asList("2018/05/10 12:10:10", LocalDate1, localDateTimeValue1), row.get("list"));
                    assertEquals(Util.map("Date", "2018/05/10 12:10:10", "Data", LocalDate1, "Name", localDateTimeValue1), row.get("map"));
                    assertTrue("Should have another row",r.hasNext());
                    row = r.next();
                    assertEquals(3L, row.get("lineNo"));
                    assertEquals(asList(null, LocalDate1, time), row.get("list"));
                    assertEquals(Util.map("Date", null, "Data", LocalDate1, "Name", time), row.get("map"));
                    assertTrue("Should have another row",r.hasNext());
                    row = r.next();
                    assertEquals(4L, row.get("lineNo"));
                    assertEquals(asList("2011-01-01T12:00:00.05381+01:00", null, null), row.get("list"));
                    assertEquals(Util.map("Date", "2011-01-01T12:00:00.05381+01:00", "Data", null, "Name", null), row.get("map"));
                    assertFalse("Should not have another row",r.hasNext());
                });
    }

    @Test
    public void testLoadXlsDateWithMappingArrayTypeDateTime() throws Exception {

        LocalDateTime localDateTimeValue = LocalDateTime.of(2018,5,10, 12,10,10, 0);
        LocalDateTime localDateTimeValue1 = LocalDateTime.of(2018,10,10, 0,0,0, 0);

        List pattern = asList("wrongPath", "dd-MM-yyyy", "dd/MM/yyyy", "yyyy/MM/dd'T'HH:mm:ss", "yyyy/dd/MM", "iso_local_date_time");

        testResult(db, "CALL apoc.load.xls($url,'dateTime',{mapping:{Date:{type: 'LOCAL_DATE_TIME', dateParse: $pattern}}})", map("url",testDate, "pattern", pattern),
                (r) -> {
                    Map<String, Object> row = r.next();
                    assertEquals(0L, row.get("lineNo"));
                    assertEquals(asList(localDateTimeValue), row.get("list"));
                    assertEquals(Util.map("Date", localDateTimeValue), row.get("map"));
                    assertTrue("Should have another row", r.hasNext());
                    row = r.next();
                    assertEquals(1L, row.get("lineNo"));
                    assertEquals(asList(localDateTimeValue1), row.get("list"));
                    assertEquals(Util.map("Date", localDateTimeValue1), row.get("map"));
                    assertFalse("Should not have another row",r.hasNext());
                });
    }

    @Test
    public void testLoadXlsDateWithMappingArrayTypeZoneDateTime() throws Exception {

        ZonedDateTime zonedDateTime = ZonedDateTime.of(LocalDateTime.of(2011,1,1,12,0,0, 53810000), ZoneOffset.of("+05:00"));

        List pattern = asList("wrongPath", "dd-MM-yyyy", "dd/MM/yyyy", "yyyy/MM/dd'T'HH:mm:ss", "yyyy/dd/MM", "iso_zoned_date_time");

        testResult(db, "CALL apoc.load.xls($url,'zonedDateTime',{mapping:{Date:{type: 'DATE_TIME', dateParse: $pattern}}})", map("url",testDate, "pattern", pattern),
                (r) -> {
                    Map<String, Object> row = r.next();
                    assertEquals(0L, row.get("lineNo"));
                    assertEquals(asList(zonedDateTime), row.get("list"));
                    assertEquals(Util.map("Date", zonedDateTime), row.get("map"));
                    assertFalse("Should not have another row", r.hasNext());
                });
    }

    @Test(expected = RuntimeException.class)
    public void testLoadXlsDateWithMappingArrayTypeZoneDateTimeWithError() throws Exception {

        List pattern = asList("wrongPath", "dd-MM-yyyy", "dd/MM/yyyy", "yyyy/MM/dd'T'HH:mm:ss", "yyyy/dd/MM", "iso_local_date_time");

        try {
            testCall(db, "CALL apoc.load.xls($url,'dateTime',{mapping:{Date:{type: 'DATE_TIME', dateParse: $pattern}}})", map("url",testDate, "pattern", pattern), (r) -> { });
        } catch (Exception e) {
            Throwable except = ExceptionUtils.getRootCause(e);
            assertTrue(except instanceof RuntimeException);
            assertEquals("Can't format the date with the pattern", except.getMessage());
            throw e;
        }
    }

    @Test
    public void testLoadXlsColumnsAfterZ() {
        testCall(db, "CALL apoc.load.xls($url, 'Sheet1!A1:AY10') yield list\n" +
                "return *",
                map("url", testColumnsAfterZ),
                (row) -> {
                    List<String> list = (List<String>) row.get("list");
                    assertEquals(51, list.size());
                    assertEquals("Details Value", list.get(50));
                });
    }

}
