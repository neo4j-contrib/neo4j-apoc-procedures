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
  public void testParseLongWithDefaultSettings() throws Exception {
    testCall(db, "CALL apoc.number.parseLong('12,345')", row -> assertEquals(new Long(12345), (Long) row.get("value")));
  }
  
  @Test
  public void testParseLongWithLocaleIT() throws Exception {
    testCall(db, "CALL apoc.number.parseLong.lang('12.345', 'it')", row -> assertEquals(new Long(12345), (Long) row.get("value")));
  }
  
  @Test
  public void testParseLongWithAPattern() throws Exception {
    testCall(db, "CALL apoc.number.parseLong.pattern('12,345', '#,##0.00;(#,##0.00)')", row -> assertEquals(new Long(12345), (Long) row.get("value")));
  }
  
  @Test
  public void testParseLongWithAPatternAndLocaleIT() throws Exception {
    testCall(db, "CALL apoc.number.parseLong.pattern.lang('12.345', '#,##0.00;(#,##0.00)', 'it')", row -> assertEquals(new Long(12345), (Long) row.get("value")));
  }

  @Test
  public void testParseLongRaisingException() throws Exception {
    expected.expect(QueryExecutionException.class);
    testCall(db, "CALL apoc.number.parseLong('aaa')", null);
  }
  
  // Parse Double
  
  @Test
  public void testParseDouble() throws Exception {
    testCall(db, "CALL apoc.number.parseDouble('12,345.67')", row -> assertEquals(new Double(12345.67), (Double) row.get("value")));
  }
  
  @Test
  public void testParseDoubleByLanguageIT() throws Exception {
    testCall(db, "CALL apoc.number.parseDouble.lang('12.345,67', 'it')", row -> assertEquals(new Double(12345.67), (Double) row.get("value")));
  }
  
  @Test
  public void testParseDoubleByPattern() throws Exception {
    testCall(db, "CALL apoc.number.parseDouble.pattern('12,345.67', '#,##0.00;(#,##0.00)')", row -> assertEquals(new Double(12345.67), (Double) row.get("value")));
  }
  
  @Test
  public void testParseDoubleByPatternAndLanguageIT() throws Exception {
    testCall(db, "CALL apoc.number.parseDouble.pattern.lang('12.345,67', '#,##0.00;(#,##0.00)', 'it')", row -> assertEquals(new Double(12345.67), (Double) row.get("value")));
  }
  
  @Test
  public void testParseDoubleRaisingException() throws Exception {
    expected.expect(QueryExecutionException.class);
    testCall(db, "CALL apoc.number.parseDouble('aaa')", null);
  }
}
