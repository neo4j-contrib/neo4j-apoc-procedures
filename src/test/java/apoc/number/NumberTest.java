package apoc.number;

import static apoc.util.TestUtil.testCall;
import static org.junit.Assert.assertEquals;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.test.TestGraphDatabaseFactory;

import apoc.util.TestUtil;

/**
 * 
 * @since 25.8.2016
 * @author inserpio
 */
public class NumberTest {

  @Rule
  public ExpectedException expected = ExpectedException.none();

  private static GraphDatabaseService db;

  @BeforeClass
  public static void sUp() throws Exception {
    db = new TestGraphDatabaseFactory().newImpermanentDatabase();
    TestUtil.registerProcedure(db, Number.class);
  }

  @AfterClass
  public static void tearDown() {
    db.shutdown();
  }

  // Format Long or Double
  
  @Test
  public void testFormatLong() throws Exception {
    testCall(db, "CALL apoc.number.format(12345)", row -> assertEquals("12,345", (String) row.get("value")));
  }

  @Test
  public void testFormatNaN() throws Exception {
    expected.expect(QueryExecutionException.class);
    testCall(db, "CALL apoc.number.format('aaa')", null);
  }
  
  @Test
  public void testFormatLongByLanguageIT() throws Exception {
    testCall(db, "CALL apoc.number.format.lang(12345, 'it')", row -> assertEquals("12.345", (String) row.get("value")));
  }
  
  @Test
  public void testFormatLongWithAnInvalidISOLanguage() throws Exception {
    expected.expect(QueryExecutionException.class);
    testCall(db, "CALL apoc.number.format.lang(12345, 'apoc')", null);
  }
  
  @Test
  public void testFormatLongByPattern() throws Exception {
    testCall(db, "CALL apoc.number.format.pattern(12345, '#,##0.00;(#,##0.00)')", row -> assertEquals("12,345.00", (String) row.get("value")));
  }
  
  @Test
  public void testFormatLongByPatternAndLanguageIT() throws Exception {
    testCall(db, "CALL apoc.number.format.pattern.lang(12345, '#,##0.00;(#,##0.00)', 'it')", row -> assertEquals("12.345,00", (String) row.get("value")));
  }
  
  @Test
  public void testFormatDouble() throws Exception {
    testCall(db, "CALL apoc.number.format(12345.67)", row -> assertEquals("12,345.67", (String) row.get("value")));
  }
  
  @Test
  public void testFormatDoubleByLanguageIT() throws Exception {
    testCall(db, "CALL apoc.number.format.lang(12345.67, 'it')", row -> assertEquals("12.345,67", (String) row.get("value")));
  }
  
  @Test
  public void testFormatDoubleByPattern() throws Exception {
    testCall(db, "CALL apoc.number.format.pattern(12345.67, '#,##0.00;(#,##0.00)')", row -> assertEquals("12,345.67", (String) row.get("value")));
  }
  
  @Test
  public void testFormatDoubleByPatternAndLanguageIT() throws Exception {
    testCall(db, "CALL apoc.number.format.pattern.lang(12345.67, '#,##0.00;(#,##0.00)', 'it')", row -> assertEquals("12.345,67", (String) row.get("value")));
  }
  
  // Parse Long
  
  @Test
  public void testParseIntWithDefaultSettings() throws Exception {
    testCall(db, "CALL apoc.number.parseInt('12,345')", row -> assertEquals(new Long(12345), (Long) row.get("value")));
  }
  
  @Test
  public void testParseIntWithLocaleIT() throws Exception {
    testCall(db, "CALL apoc.number.parseInt.lang('12.345', 'it')", row -> assertEquals(new Long(12345), (Long) row.get("value")));
  }
  
  @Test
  public void testParseIntWithAPattern() throws Exception {
    testCall(db, "CALL apoc.number.parseInt.pattern('12,345', '#,##0.00;(#,##0.00)')", row -> assertEquals(new Long(12345), (Long) row.get("value")));
  }
  
  @Test
  public void testParseIntWithAPatternAndLocaleIT() throws Exception {
    testCall(db, "CALL apoc.number.parseInt.pattern.lang('12.345', '#,##0.00;(#,##0.00)', 'it')", row -> assertEquals(new Long(12345), (Long) row.get("value")));
  }

  @Test
  public void testParseIntRaisingException() throws Exception {
    expected.expect(QueryExecutionException.class);
    testCall(db, "CALL apoc.number.parseInt('aaa')", null);
  }
  
  // Parse Double
  
  @Test
  public void testParseFloat() throws Exception {
    testCall(db, "CALL apoc.number.parseFloat('12,345.67')", row -> assertEquals(new Double(12345.67), (Double) row.get("value")));
  }
  
  @Test
  public void testParseFloatByLanguageIT() throws Exception {
    testCall(db, "CALL apoc.number.parseFloat.lang('12.345,67', 'it')", row -> assertEquals(new Double(12345.67), (Double) row.get("value")));
  }
  
  @Test
  public void testParseFloatByPattern() throws Exception {
    testCall(db, "CALL apoc.number.parseFloat.pattern('12,345.67', '#,##0.00;(#,##0.00)')", row -> assertEquals(new Double(12345.67), (Double) row.get("value")));
  }
  
  @Test
  public void testParseFloatByPatternAndLanguageIT() throws Exception {
    testCall(db, "CALL apoc.number.parseFloat.pattern.lang('12.345,67', '#,##0.00;(#,##0.00)', 'it')", row -> assertEquals(new Double(12345.67), (Double) row.get("value")));
  }
  
  @Test
  public void testParseFloatRaisingException() throws Exception {
    expected.expect(QueryExecutionException.class);
    testCall(db, "CALL apoc.number.parseFloat('aaa')", null);
  }
}
