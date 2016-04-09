package apoc.date;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.stream.Stream;
import apoc.util.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.test.TestGraphDatabaseFactory;
import static apoc.util.TestUtil.testCall;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.*;


public class DateTest {
	@Rule public ExpectedException expected = ExpectedException.none();
	private GraphDatabaseService db;
	private DateFormat defaultFormat = formatInUtcZone("yyyy-MM-dd HH:mm:ss");
	private String epochAsString = defaultFormat.format(new java.util.Date(0L));

	@Before
	public void sUp() throws Exception {
		db = new TestGraphDatabaseFactory().newImpermanentDatabase();
		TestUtil.registerProcedure(db, Date.class);
	}

	@After
	public void tearDown() {
		db.shutdown();
	}

	@Test public void testToUnixtime() throws Exception {
		testCall(db,
				"CALL apoc.date.toSeconds('" + epochAsString + "') yield value as dob RETURN dob",
				row -> assertEquals(Instant.EPOCH, Instant.ofEpochSecond((long) row.get("dob"))));
	}

	@Test public void testToUnixtimeWithCorrectFormat() throws Exception {
		String pattern = "HH:mm:ss/yyyy";
		SimpleDateFormat customFormat = formatInUtcZone(pattern);
		String reference = customFormat.format(new java.util.Date(0L));
		testCall(db,
				"CALL apoc.date.toSecondsFormatted('" + reference + "', '" + pattern + "') yield value as dob RETURN dob",
				row -> assertEquals(Instant.EPOCH, Instant.ofEpochSecond((long) row.get("dob"))));
	}

	@Test public void testToUnixtimeWithIncorrectPatternFormat() throws Exception {
		expected.expect(instanceOf(QueryExecutionException.class));
		testCall(db,
				"CALL apoc.date.toSecondsFormatted('12:12:12/1945', 'HH:mm:ss/yyyy/neo4j') yield value as dob RETURN dob",
				row -> assertEquals(Instant.EPOCH, Instant.ofEpochSecond((long) row.get("dob"))));
	}

	@Test public void testToUnixtimeWithNullInput() throws Exception {
		testCall(db,
				"CALL apoc.date.toSeconds(NULL) yield value as dob RETURN dob",
				row -> assertNull(row.get("dob")));
	}

	@Test public void testFromUnixtime() throws Exception {
		testCall(db,
				"CALL apoc.date.fromSeconds(0) yield value as dob RETURN dob",
				row -> {
					try {
						assertEquals(new java.util.Date(0L), defaultFormat.parse((String) row.get("dob")));
					} catch (ParseException e) {
						throw new RuntimeException(e);
					}
				});
	}

	@Test public void testFromUnixtimeWithCorrectFormat() throws Exception {
		String pattern = "HH:mm:ss/yyyy";
		SimpleDateFormat customFormat = formatInUtcZone(pattern);
		testCall(db,
				"CALL apoc.date.fromSecondsFormatted(0, '" + pattern + "') yield value as dob RETURN dob",
				row -> {
					try {
						assertEquals(new java.util.Date(0L), customFormat.parse((String) row.get("dob")));
					} catch (ParseException e) {
						throw new RuntimeException(e);
					}
				});
	}

	@Test public void testFromUnixtimeWithIncorrectPatternFormat() throws Exception {
		expected.expect(instanceOf(QueryExecutionException.class));
		testCall(db,
				"CALL apoc.date.fromSecondsFormatted(0, 'HH:mm:ss/yyyy/neo4j') yield value as dob RETURN dob",
				row -> {});
	}

	@Test public void testFromUnixtimeWithNegativeInput() throws Exception {
		expected.expect(instanceOf(QueryExecutionException.class));
		testCall(db, "CALL apoc.date.toSeconds(-1) yield value as dob RETURN dob", row -> {});
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
			String query = "MATCH (p:Person) WITH p CALL apoc.date.toSeconds(p.born) yield value as dob RETURN p,dob ORDER BY dob ";
			actual = Iterators.asList(db.execute(query).<Long>columnAs("dob")).stream()
					.map(dob -> java.util.Date.from(Instant.ofEpochSecond((long) dob)))
					.collect(toList());
			tx.success();
		}

		assertEquals(expected, actual);
	}

	@Test
	public void testListFields() throws Exception {
		testCall(db,
				"CALL apoc.date.listFields('2015-01-02 03:04:05') yield value as m RETURN m",
				row -> {
					Map<String, Object> split = (Map<String, Object>) row.get("m");
					assertEquals(2015L, split.get("Years"));
					assertEquals(1L, split.get("Months"));
					assertEquals(2L, split.get("Days"));
					assertEquals(3L, split.get("Hours"));
					assertEquals(4L, split.get("Minutes"));
					assertEquals(5L, split.get("Seconds"));
				});
	}

	@Test
	public void testListFieldsCustomFormat() throws Exception {
		testCall(db,
				"CALL apoc.date.listFieldsFormatted('2015-01-02 03:04:05 EET', 'yyyy-MM-dd HH:mm:ss zzz') yield value as m RETURN m",
				row -> {
					Map<String, Object> split = (Map<String, Object>) row.get("m");
					assertEquals(2015L, split.get("Years"));
					assertEquals(1L, split.get("Months"));
					assertEquals(2L, split.get("Days"));
					assertEquals(3L, split.get("Hours"));
					assertEquals(4L, split.get("Minutes"));
					assertEquals(5L, split.get("Seconds"));
					assertEquals("Europe/Bucharest", split.get("ZoneId"));
				});

		testCall(db,
				"CALL apoc.date.listFieldsFormatted('2015/01/02/03/04/05/EET', 'yyyy/MM/dd/HH/mm/ss/z') yield value as m RETURN m",
				row -> {
					Map<String, Object> split = (Map<String, Object>) row.get("m");
					assertEquals(2015L, split.get("Years"));
					assertEquals(1L, split.get("Months"));
					assertEquals(2L, split.get("Days"));
					assertEquals(3L, split.get("Hours"));
					assertEquals(4L, split.get("Minutes"));
					assertEquals(5L, split.get("Seconds"));
					assertEquals("Europe/Bucharest", split.get("ZoneId"));
				});

		testCall(db,
				"CALL apoc.date.listFieldsFormatted('2015/01/02_EET', 'yyyy/MM/dd_z') yield value as m RETURN m",
				row -> {
					Map<String, Object> split = (Map<String, Object>) row.get("m");
					assertEquals(2015L, split.get("Years"));
					assertEquals(1L, split.get("Months"));
					assertEquals(2L, split.get("Days"));
					assertEquals("Europe/Bucharest", split.get("ZoneId"));
				});
	}

	@Test
	public void testListFieldsNullInput() throws Exception {
		testCall(db,
				"CALL apoc.date.listFieldsFormatted(NULL, 'yyyy-MM-dd HH:mm:ss zzz') yield value as m RETURN m",
				row -> {
					Map<String, Object> split = (Map<String, Object>) row.get("m");
					assertTrue(split.isEmpty());
				});
	}

	private SimpleDateFormat formatInUtcZone(final String pattern) {
		SimpleDateFormat customFormat = new SimpleDateFormat(pattern);
		customFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
		return customFormat;
	}
}
