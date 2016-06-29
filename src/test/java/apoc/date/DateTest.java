package apoc.date;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.stream.Stream;
import apoc.util.TestUtil;
import org.junit.*;
import org.junit.rules.ExpectedException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.test.TestGraphDatabaseFactory;

import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testCall;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.*;


public class DateTest {
	@Rule public ExpectedException expected = ExpectedException.none();
	private static GraphDatabaseService db;
	private DateFormat defaultFormat = formatInUtcZone("yyyy-MM-dd HH:mm:ss");
	private String epochAsString = defaultFormat.format(new java.util.Date(0L));
	private java.util.Date testDate = new java.util.Date(1464739200000L);
	private String testDateAsString = defaultFormat.format( testDate );
	private static final long SECONDS_PER_MINUTE = 60;
	private static final long SECONDS_PER_HOUR = SECONDS_PER_MINUTE * 60;
	private static final long SECONDS_PER_DAY = SECONDS_PER_HOUR * 24;

	@BeforeClass
	public static void sUp() throws Exception {
		db = new TestGraphDatabaseFactory().newImpermanentDatabase();
		TestUtil.registerProcedure(db, Date.class);
	}

	@AfterClass
	public static void tearDown() {
		db.shutdown();
	}

	@Test public void testToDays() throws Exception {
		testCall(db,
				"CALL apoc.date.parseDefault({date},'d')",
				map("date",testDateAsString),
				row -> assertEquals(testDate.toInstant(), Instant.ofEpochSecond (SECONDS_PER_DAY * (long) row.get("value"))));
	}

	@Test public void testToHours() throws Exception {
		testCall(db,
				"CALL apoc.date.parseDefault({date},'h')",
				map("date",testDateAsString),
				row -> assertEquals(testDate.toInstant(), Instant.ofEpochSecond (SECONDS_PER_HOUR * (long) row.get("value"))));
	}

	@Test public void testToMinutes() throws Exception {
		testCall(db,
				"CALL apoc.date.parseDefault({date},'m')",
				map("date",testDateAsString),
				row -> assertEquals(testDate.toInstant(), Instant.ofEpochSecond (SECONDS_PER_MINUTE * (long) row.get("value"))));
	}

	@Test public void testToUnixtime() throws Exception {
		testCall(db,
				"CALL apoc.date.parseDefault({date},'s')",
				map("date",epochAsString),
				row -> assertEquals(Instant.EPOCH, Instant.ofEpochSecond((long) row.get("value"))));
	}

	@Test public void testToMillis() throws Exception {
		testCall(db,
				"CALL apoc.date.parseDefault({date},'ms')",
				map("date",epochAsString),
				row -> assertEquals(Instant.EPOCH, Instant.ofEpochMilli((long) row.get("value"))));
	}

	@Test public void testToUnixtimeWithCorrectFormat() throws Exception {
		String pattern = "MM/dd/yyyy HH:mm:ss";
		SimpleDateFormat customFormat = formatInUtcZone(pattern);
		String reference = customFormat.format(new java.util.Date(0L));
		testCall(db,
				"CALL apoc.date.parse({date},'s',{pattern})",
				map("date",reference,"pattern",pattern),
				row -> assertEquals(Instant.EPOCH, Instant.ofEpochSecond((long) row.get("value"))));
	}

	@Test public void testToUnixtimeWithIncorrectPatternFormat() throws Exception {
		expected.expect(instanceOf(QueryExecutionException.class));
		testCall(db,
				"CALL apoc.date.parse('12/12/1945 12:12:12','s','MM/dd/yyyy HH:mm:ss/neo4j')",
				row -> assertEquals(Instant.EPOCH, Instant.ofEpochSecond((long) row.get("value"))));
	}

	@Test public void testToUnixtimeWithNullInput() throws Exception {
		testCall(db,
				"CALL apoc.date.parseDefault(NULL,'s')",
				row -> assertNull(row.get("value")));
	}

	@Test public void testFromUnixtime() throws Exception {
		testCall(db,
				"CALL apoc.date.formatDefault(0,'s')",
				row -> {
					try {
						assertEquals(new java.util.Date(0L), defaultFormat.parse((String) row.get("value")));
					} catch (ParseException e) {
						throw new RuntimeException(e);
					}
				});
	}

	@Test public void testFromUnixtimeWithCorrectFormat() throws Exception {
		String pattern = "MM/dd/yyyy HH:mm:ss";
		SimpleDateFormat customFormat = formatInUtcZone(pattern);
		testCall(db,
				"CALL apoc.date.format(0,'s',{pattern})",
				map("pattern",pattern),
				row -> {
					try {
						assertEquals(new java.util.Date(0L), customFormat.parse((String) row.get("value")));
					} catch (ParseException e) {
						throw new RuntimeException(e);
					}
				});
	}

	@Test public void testFromUnixtimeWithCorrectFormatAndTimeZone() throws Exception {
		String pattern = "MM/dd/yyyy HH:mm:ss";
		String timezone = "America/New_York";
		SimpleDateFormat customFormat = formatInCustomTimeZone(pattern, timezone);
		testCall(db,
				"CALL apoc.date.formatTimeZone(0,'s',{pattern},{timezone})",
				map("pattern",pattern,"timezone",timezone),
				row -> {
					try {
						assertEquals(new java.util.Date(0L), customFormat.parse((String) row.get("value")));
					} catch (ParseException e) {
						throw new RuntimeException(e);
					}
				});
	}

	@Test public void testFromUnixtimeWithIncorrectPatternFormat() throws Exception {
		expected.expect(instanceOf(QueryExecutionException.class));
		testCall(db,
				"CALL apoc.date.format(0,'s','MM/dd/yyyy HH:mm:ss/neo4j')",
				row -> {});
	}

	@Test public void testFromUnixtimeWithIncorrectPatternFormatAndTimeZone() throws Exception {
		expected.expect(instanceOf(QueryExecutionException.class));
		testCall(db,
				"CALL apoc.date.formatTimeZone(0,'s','MM/dd/yyyy HH:mm:ss/neo4j','Neo4j/Apoc')",
				row -> {});
	}

	@Test public void testFromUnixtimeWithNegativeInput() throws Exception {
		expected.expect(instanceOf(QueryExecutionException.class));
		testCall(db, "CALL apoc.date.formatDefault(-1,'s')", row -> {});
	}

	@Test public void testOrderByDate() throws Exception {
		SimpleDateFormat format = formatInUtcZone("yyyy-MM-dd HH:mm:ss");
		try (Transaction tx = db.beginTx()) {
			for (int i = 0 ; i < 8; i++) {
				Node datedNode = db.createNode(() -> "Person");
				datedNode.setProperty("born", format.format(java.util.Date.from(Instant.EPOCH.plus(i, ChronoUnit.DAYS))));
			}
			for (int i = 15 ; i >= 8; i--) {
				Node datedNode = db.createNode(() -> "Person");
				datedNode.setProperty("born", format.format(java.util.Date.from(Instant.EPOCH.plus(i, ChronoUnit.DAYS))));
			}
			tx.success();
		}

		List<java.util.Date> expected = Stream.iterate(Instant.EPOCH, prev -> prev.plus(1, ChronoUnit.DAYS))
				.limit(16)
				.map(java.util.Date::from)
				.sorted()
				.collect(toList());

		List<java.util.Date> actual;
		try (Transaction tx = db.beginTx()) {
			String query = "MATCH (p:Person) WITH p CALL apoc.date.parseDefault(p.born,'s') yield value as dob RETURN p,dob ORDER BY dob ";
			actual = Iterators.asList(db.execute(query).<Long>columnAs("dob")).stream()
					.map(dob -> java.util.Date.from(Instant.ofEpochSecond((long) dob)))
					.collect(toList());
			tx.success();
		}

		assertEquals(expected, actual);
	}

	@Test
	public void testfields() throws Exception {
		testCall(db,
				"CALL apoc.date.fieldsDefault('2015-01-02 03:04:05')",
				row -> {
					Map<String, Object> map = (Map<String, Object>) row.get("value");
					assertEquals(2015L, map.get("years"));
					assertEquals(2015L, row.get("years"));
					assertEquals(1L, map.get("months"));
					assertEquals(1L, row.get("months"));
					assertEquals(2L, map.get("days"));
					assertEquals(2L, row.get("days"));
					assertEquals(5L, map.get("weekdays"));
					assertEquals(5L, row.get("weekdays"));
					assertEquals(3L, map.get("hours"));
					assertEquals(3L, row.get("hours"));
					assertEquals(4L, map.get("minutes"));
					assertEquals(4L, row.get("minutes"));
					assertEquals(5L, map.get("seconds"));
					assertEquals(5L, row.get("seconds"));
				});
	}

	@Test
	public void testfieldsCustomFormat() throws Exception {
		testCall(db,
				"CALL apoc.date.fields('2015-01-02 03:04:05 EET', 'yyyy-MM-dd HH:mm:ss zzz') yield value as m RETURN m",
				row -> {
					Map<String, Object> split = (Map<String, Object>) row.get("m");
					assertEquals(2015L, split.get("years"));
					assertEquals(1L, split.get("months"));
					assertEquals(2L, split.get("days"));
					assertEquals(3L, split.get("hours"));
					assertEquals(4L, split.get("minutes"));
					assertEquals(5L, split.get("seconds"));
					assertEquals(
							TimeZone.getTimeZone("EET").getRawOffset(),
							TimeZone.getTimeZone((String)split.get("zoneid")).getRawOffset()
					);
				});

		testCall(db,
				"CALL apoc.date.fields('2015/01/02/03/04/05/EET', 'yyyy/MM/dd/HH/mm/ss/z')",
				row -> {
					Map<String, Object> split = (Map<String, Object>) row.get("value");
					assertEquals(2015L, split.get("years"));
					assertEquals(1L, split.get("months"));
					assertEquals(2L, split.get("days"));
					assertEquals(3L, split.get("hours"));
					assertEquals(4L, split.get("minutes"));
					assertEquals(5L, split.get("seconds"));
					assertEquals(
							TimeZone.getTimeZone("EET").getRawOffset(),
							TimeZone.getTimeZone((String)split.get("zoneid")).getRawOffset()
					);
				});

		testCall(db,
				"CALL apoc.date.fields('2015/01/02_EET', 'yyyy/MM/dd_z')",
				row -> {
					Map<String, Object> split = (Map<String, Object>) row.get("value");
					assertEquals(2015L, split.get("years"));
					assertEquals(1L, split.get("months"));
					assertEquals(2L, split.get("days"));
					assertEquals(
							TimeZone.getTimeZone("EET").getRawOffset(),
							TimeZone.getTimeZone((String)split.get("zoneid")).getRawOffset()
					);
				});
	}

	@Test
	public void testfieldsNullInput() throws Exception {
		testCall(db,
				"CALL apoc.date.fields(NULL, 'yyyy-MM-dd HH:mm:ss zzz')",
				row -> {
					Map<String, Object> split = (Map<String, Object>) row.get("value");
					assertTrue(split.isEmpty());
				});
	}

	private SimpleDateFormat formatInUtcZone(final String pattern) {
		SimpleDateFormat customFormat = new SimpleDateFormat(pattern);
		customFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
		return customFormat;
	}

	private SimpleDateFormat formatInCustomTimeZone(final String pattern, final String timezone) {
		SimpleDateFormat customFormat = new SimpleDateFormat(pattern);
		customFormat.setTimeZone(TimeZone.getTimeZone(timezone));
		return customFormat;
	}
}
