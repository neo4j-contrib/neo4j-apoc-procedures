package apoc.load;

import apoc.load.LoadCsv;
import apoc.util.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.net.URL;
import java.util.Map;

import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testResult;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class LoadCsvTest {

    private GraphDatabaseService db;
    @Before public void setUp() throws Exception {
        db = new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder().setConfig("apoc.import.file.enabled","true").newGraphDatabase();
        TestUtil.registerProcedure(db, LoadCsv.class);
    }

    @After public void tearDown() {
        db.shutdown();
    }

    @Test public void testLoadCsv() throws Exception {
        URL url = ClassLoader.getSystemResource("test.csv");
        testResult(db, "CALL apoc.load.csv({url},null)", map("url",url.toString()), // 'file:test.csv'
                (r) -> {
                    assertRow(r, "Selma", "8", 0L);
                    assertRow(r, "Rana", "11", 1L);
                    assertRow(r, "Selina", "18", 2L);
                    assertEquals(false, r.hasNext());
                });
    }

    private void assertRow(Result r, String name, String age, long lineNo) {
        Map<String, Object> row = r.next();
        assertEquals(map("name", name,"age", age), row.get("map"));
        assertEquals(asList(name, age), row.get("list"));
        assertEquals(lineNo, row.get("lineNo"));
    }

    @Test public void testLoadCsvSkip() throws Exception {
        URL url = ClassLoader.getSystemResource("test.csv");
        testResult(db, "CALL apoc.load.csv({url},{skip:1,limit:1})", map("url",url.toString()), // 'file:test.csv'
                (r) -> {
                    assertRow(r, "Rana", "11", 1L);
                    assertEquals(false, r.hasNext());
                });
    }
    @Test public void testLoadCsvTabSeparator() throws Exception {
        URL url = ClassLoader.getSystemResource("test-tab.csv");
        testResult(db, "CALL apoc.load.csv({url},{sep:'TAB'})", map("url",url.toString()), // 'file:test.csv'
                (r) -> {
                    assertRow(r, "Rana", "11", 0L);
                    assertEquals(false, r.hasNext());
                });
    }

    @Test public void testLoadCsvNoHeader() throws Exception {
        URL url = ClassLoader.getSystemResource("test-no-header.csv");
        testResult(db, "CALL apoc.load.csv({url},{header:false})", map("url",url.toString()), // 'file:test.csv'
                (r) -> {
                    Map<String, Object> row = r.next();
                    assertEquals(null, row.get("map"));
                    assertEquals(asList("Selma", "8"), row.get("list"));
                    assertEquals(0L, row.get("lineNo"));
                    assertEquals(false, r.hasNext());
                });
    }
    @Test public void testLoadCsvIgnoreFields() throws Exception {
        URL url = ClassLoader.getSystemResource("test-tab.csv");
        testResult(db, "CALL apoc.load.csv({url},{ignore:['age'],sep:'TAB'})", map("url",url.toString()), // 'file:test.csv'
                (r) -> {
                    Map<String, Object> row = r.next();
                    assertEquals(map("name", "Rana"), row.get("map"));
                    assertEquals(asList("Rana"), row.get("list"));
                    assertEquals(0L, row.get("lineNo"));
                    assertEquals(false, r.hasNext());
                });
    }

    @Test public void testLoadCsvColonSeparator() throws Exception {
        URL url = ClassLoader.getSystemResource("test.dsv");
        testResult(db, "CALL apoc.load.csv({url},{sep:':'})", map("url",url.toString()), // 'file:test.csv'
                (r) -> {
                    assertRow(r,"Rana","11",0L);
                    assertEquals(false, r.hasNext());
                });
    }

    @Test public void testMapping() throws Exception {
        URL url = ClassLoader.getSystemResource("test-mapping.csv");
        testResult(db, "CALL apoc.load.csv({url},{mapping:{name:{type:'string'},age:{type:'int'},kids:{array:true,arraySep:':',type:'int'},pass:{ignore:true}}})", map("url",url.toString()), // 'file:test.csv'
                (r) -> {
                    Map<String, Object> row = r.next();
                    assertEquals(map("name", "Michael", "age", 41L, "kids", asList(8L, 11L, 18L)), row.get("map"));
                    assertEquals(asList("Michael", 41L, asList(8L, 11L, 18L)), row.get("list"));
                    assertEquals(0L, row.get("lineNo"));
                    assertEquals(false, r.hasNext());
                });
    }

    @Test
    public void testLoadCsvByUrl() throws Exception {
        URL url = new URL("https://raw.githubusercontent.com/neo4j-contrib/neo4j-apoc-procedures/3.1/src/test/resources/test.csv");
        testResult(db, "CALL apoc.load.csv({url},null)", map("url", url.toString()),
                (r) -> {
                    assertRow(r, "Selma", "8", 0L);
                    assertRow(r, "Rana", "11", 1L);
                    assertRow(r, "Selina", "18", 2L);
                    assertEquals(false, r.hasNext());
                });

    }

    @Test
    public void testLoadCsvByUrlRedirect() throws Exception {
        URL url = new URL("http://bit.ly/2nXgHA2");
        testResult(db, "CALL apoc.load.csv({url},null)", map("url", url.toString()),
                (r) -> {
                    assertRow(r, "Selma", "8", 0L);
                    assertRow(r, "Rana", "11", 1L);
                    assertRow(r, "Selina", "18", 2L);
                    assertEquals(false, r.hasNext());
                });
    }
}
