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

  @Test
  public void testFormatLongWithDefaultSettings() throws Exception {
    testCall(db, "CALL apoc.number.formatLong(12345, null, null)", row -> assertEquals("12,345", (String) row.get("value")));
  }

  @Test
  public void testFormatLongWithLanguageIT() throws Exception {
    testCall(db, "CALL apoc.number.formatLong(12345, null, 'it')", row -> assertEquals("12.345", (String) row.get("value")));
  }
  
  @Test
  public void testFormatLongWithAnInvalidISOLanguage() throws Exception {
    expected.expect(QueryExecutionException.class);
    testCall(db, "CALL apoc.number.formatLong(12345, null, 'apoc')", null);
  }
  
  @Test
  public void testFormatLongWithAPattern() throws Exception {
    testCall(db, "CALL apoc.number.formatLong(12345, '#,##0.00;(#,##0.00)', null)", row -> assertEquals("12,345.00", (String) row.get("value")));
  }
  
  @Test
  public void testFormatLongWithAPatternAndLocaleIT() throws Exception {
    testCall(db, "CALL apoc.number.formatLong(12345, '#,##0.00;(#,##0.00)', 'it')", row -> assertEquals("12.345,00", (String) row.get("value")));
  }
  
  @Test
  public void testFormatDoubleWithDefaultSettings() throws Exception {
    testCall(db, "CALL apoc.number.formatDouble(12345.67, null, null)", row -> assertEquals("12,345.67", (String) row.get("value")));
  }
  
  @Test
  public void testFormatDoubleWithLocaleIT() throws Exception {
    testCall(db, "CALL apoc.number.formatDouble(12345.67, null, 'it')", row -> assertEquals("12.345,67", (String) row.get("value")));
  }
  
  @Test
  public void testFormatDoubleWithAPattern() throws Exception {
    testCall(db, "CALL apoc.number.formatDouble(12345.67, '#,##0.00;(#,##0.00)', null)", row -> assertEquals("12,345.67", (String) row.get("value")));
  }
  
  @Test
  public void testFormatDoubleWithAPatternAndLocaleIT() throws Exception {
    testCall(db, "CALL apoc.number.formatDouble(12345.67, '#,##0.00;(#,##0.00)', 'it')", row -> assertEquals("12.345,67", (String) row.get("value")));
  }
  
  @Test
  public void testParseAsLongWithDefaultSettings() throws Exception {
    testCall(db, "CALL apoc.number.parseAsLong('12,345', null, null)", row -> assertEquals(new Long(12345), (Long) row.get("value")));
  }
  
  @Test
  public void testParseAsLongWithLocaleIT() throws Exception {
    testCall(db, "CALL apoc.number.parseAsLong('12.345', null, 'it')", row -> assertEquals(new Long(12345), (Long) row.get("value")));
  }
  
  @Test
  public void testParseAsLongWithAPattern() throws Exception {
    testCall(db, "CALL apoc.number.parseAsLong('12,345', '#,##0.00;(#,##0.00)', null)", row -> assertEquals(new Long(12345), (Long) row.get("value")));
  }
  
  @Test
  public void testParseAsLongWithAPatternAndLocaleIT() throws Exception {
    testCall(db, "CALL apoc.number.parseAsLong('12.345', '#,##0.00;(#,##0.00)', 'it')", row -> assertEquals(new Long(12345), (Long) row.get("value")));
  }
  
  @Test
  public void testParseAsLongRaisingException() throws Exception {
    expected.expect(QueryExecutionException.class);
    testCall(db, "CALL apoc.number.parseAsLong('aaa', null, null)", null);
  }
  
  @Test
  public void testParseAsDoubleWithDefaultSettings() throws Exception {
    testCall(db, "CALL apoc.number.parseAsDouble('12,345.67', null, null)", row -> assertEquals(new Double(12345.67), (Double) row.get("value")));
  }
  
  @Test
  public void testParseAsDoubleWithLocaleIT() throws Exception {
    testCall(db, "CALL apoc.number.parseAsDouble('12.345,67', null, 'it')", row -> assertEquals(new Double(12345.67), (Double) row.get("value")));
  }
  
  @Test
  public void testParseAsDoubleWithAPattern() throws Exception {
    testCall(db, "CALL apoc.number.parseAsDouble('12,345.67', '#,##0.00;(#,##0.00)', null)", row -> assertEquals(new Double(12345.67), (Double) row.get("value")));
  }
  
  @Test
  public void testParseAsDoubleWithAPatternAndLocaleIT() throws Exception {
    testCall(db, "CALL apoc.number.parseAsDouble('12.345,67', '#,##0.00;(#,##0.00)', 'it')", row -> assertEquals(new Double(12345.67), (Double) row.get("value")));
  }
  
  @Test
  public void testParseAsDoubleRaisingException() throws Exception {
    expected.expect(QueryExecutionException.class);
    testCall(db, "CALL apoc.number.parseAsDouble('aaa', null, null)", null);
  }
  
  @Test
  public void testDefaultNegativePrefix() throws Exception {
    testCall(db, "CALL apoc.number.defaultNegativePrefix()", row -> assertEquals("-", (String) row.get("value")));
  }

  @Test
  public void testDefaultPositivePrefix() throws Exception {
    testCall(db, "CALL apoc.number.defaultPositivePrefix()", row -> assertEquals("", (String) row.get("value")));
  }

  @Test
  public void testDefaultGroupingSize() throws Exception {
    testCall(db, "CALL apoc.number.defaultGroupingSize()", row -> assertEquals(3, (long) row.get("value")));
  }
}
