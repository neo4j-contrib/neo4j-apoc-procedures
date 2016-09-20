package apoc.number;

import static apoc.util.TestUtil.testCall;
import static org.junit.Assert.assertEquals;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.test.TestGraphDatabaseFactory;

import apoc.util.TestUtil;

/**
 * 
 * @since 25.8.2016
 * @author inserpio
 */
public class NumbersTest {

  @Rule
  public ExpectedException expected = ExpectedException.none();

  private static GraphDatabaseService db;

  @BeforeClass
  public static void sUp() throws Exception {
    db = new TestGraphDatabaseFactory().newImpermanentDatabase();
    TestUtil.registerProcedure(db, Numbers.class);
  }

  @AfterClass
  public static void tearDown() {
    db.shutdown();
  }

  @Test
  public void testFormat() throws Exception {
    testCall(db, "RETURN apoc.number.format(12345) AS value", row -> assertEquals("12,345",  row.get("value")));
    testCall(db, "RETURN apoc.number.format('aaa') AS value", row -> assertEquals(null,  row.get("value")));
    testCall(db, "RETURN apoc.number.format(12345, '', 'it') AS value", row -> assertEquals("12.345",  row.get("value")));
    testCall(db, "RETURN apoc.number.format(12345, '', 'apoc') AS value", row -> assertEquals(null,  row.get("value")));
    testCall(db, "RETURN apoc.number.format(12345, '#,##0.00;(#,##0.00)') AS value", row -> assertEquals("12,345.00",  row.get("value")));
    testCall(db, "RETURN apoc.number.format(12345, '#,##0.00;(#,##0.00)', 'it') AS value", row -> assertEquals("12.345,00",  row.get("value")));
    testCall(db, "RETURN apoc.number.format(12345.67) AS value", row -> assertEquals("12,345.67",  row.get("value")));
    testCall(db, "RETURN apoc.number.format(12345.67, '', 'it') AS value", row -> assertEquals("12.345,67",  row.get("value")));
    testCall(db, "RETURN apoc.number.format(12345.67, '#,##0.00;(#,##0.00)') AS value", row -> assertEquals("12,345.67",  row.get("value")));
    testCall(db, "RETURN apoc.number.format(12345.67, '#,##0.00;(#,##0.00)', 'it') AS value", row -> assertEquals("12.345,67",  row.get("value")));
  }
  
  @Test
  public void testParseInt() throws Exception {
    testCall(db, "RETURN apoc.number.parseInt('12,345') AS value", row -> assertEquals(new Long(12345), row.get("value")));
    testCall(db, "RETURN apoc.number.parseInt('12.345', '' ,'it') AS value", row -> assertEquals(new Long(12345), row.get("value")));
    testCall(db, "RETURN apoc.number.parseInt('12,345', '#,##0.00;(#,##0.00)') AS value", row -> assertEquals(new Long(12345), row.get("value")));
    testCall(db, "RETURN apoc.number.parseInt('12.345', '#,##0.00;(#,##0.00)', 'it') AS value", row -> assertEquals(new Long(12345), row.get("value")));
    testCall(db, "RETURN apoc.number.parseInt('aaa') AS value", row -> assertEquals(null,  row.get("value")));
  }
  
  // Parse Double
  
  @Test
  public void testParseFloat() throws Exception {
    testCall(db, "RETURN apoc.number.parseFloat('12,345.67') AS value", row -> assertEquals(new Double(12345.67), row.get("value")));
    testCall(db, "RETURN apoc.number.parseFloat('12.345,67', '', 'it') AS value", row -> assertEquals(new Double(12345.67), row.get("value")));
    testCall(db, "RETURN apoc.number.parseFloat('12,345.67', '#,##0.00;(#,##0.00)') AS value", row -> assertEquals(new Double(12345.67), row.get("value")));
    testCall(db, "RETURN apoc.number.parseFloat('12.345,67', '#,##0.00;(#,##0.00)', 'it') AS value", row -> assertEquals(new Double(12345.67), row.get("value")));
    testCall(db, "RETURN apoc.number.parseFloat('aaa') AS value", row -> assertEquals(null,  row.get("value")));
  }
}
