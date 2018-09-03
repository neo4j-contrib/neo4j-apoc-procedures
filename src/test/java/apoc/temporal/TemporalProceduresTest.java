package apoc.temporal;

import apoc.util.TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.neo4j.test.TestGraphDatabaseFactory;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;

import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertEquals;

public class TemporalProceduresTest
{
    @Rule public ExpectedException expected = ExpectedException.none();
    private static GraphDatabaseService db;

    @BeforeClass public static void setUp() throws Exception {
        db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        TestUtil.registerProcedure(db, TemporalProcedures.class);
    }

    @AfterClass public static void tearDown() {
        db.shutdown();
    }


    @Test
    public void shouldFormatDate() throws Throwable
    {
        try (Transaction tx = db.beginTx() ) {
            Result res = db.execute("RETURN apoc.temporal.format( date( { year: 2018, month: 12, day: 10 } ), \"yyyy-MM-dd\" ) as output");

            assertEquals("2018-12-10", res.next().get("output"));
        }
    }


    @Test
    public void shouldFormatDateTime() throws Throwable
    {
        try (Transaction tx = db.beginTx() ) {
            Result res = db.execute("RETURN apoc.temporal.format( datetime( { year: 2018, month: 12, day: 10, hour: 12, minute: 34, second: 56, nanosecond: 123456789 } ), \"yyyy-MM-dd'T'HH:mm:ss.SSSS\" ) as output");

            assertEquals("2018-12-10T12:34:56.1234", res.next().get("output"));
        }
    }

    @Test
    public void shouldFormatLocalDateTime() throws Throwable
    {
        try (Transaction tx = db.beginTx() ) {
            Result res = db.execute("RETURN apoc.temporal.format( localdatetime( { year: 2018, month: 12, day: 10, hour: 12, minute: 34, second: 56, nanosecond: 123456789 } ), \"yyyy-MM-dd'T'HH:mm:ss.SSSS\" ) as output");

            assertEquals("2018-12-10T12:34:56.1234", res.next().get("output"));
        }
    }

    @Test
    public void shouldFormatTime() throws Throwable
    {
        try (Transaction tx = db.beginTx() ) {
            Result res = db.execute("RETURN apoc.temporal.format( time( { hour: 12, minute: 34, second: 56, nanosecond: 123456789, timezone: 'Europe/London' } ), \"HH:mm:ss.SSSSZ\" ) as output");

            assertEquals( "12:34:56.1234+0100" , res.next().get("output"));
        }
    }

    @Test
    public void shouldFormatLocalTime() throws Throwable
    {
        try (Transaction tx = db.beginTx() ) {
            Result res = db.execute("RETURN apoc.temporal.format( localtime( { hour: 12, minute: 34, second: 56, nanosecond: 123456789 } ), \"HH:mm:ss.SSSS\" ) as output");

            assertEquals("12:34:56.1234" , res.next().get("output"));

        }
    }

    @Test
    public void shouldFormatDuration() throws Throwable
    {
        try (Transaction tx = db.beginTx() ) {
            Result res = db.execute("RETURN apoc.temporal.format( duration('P0M0DT4820.487660000S'), \"HH:mm:ss.SSSS\" ) as output");

            assertEquals("01:20:20.4876", res.next().get("output"));
        }
    }

    @Test
    public void shouldFormatDurationTemporal() throws Throwable
    {
        try (Transaction tx = db.beginTx() ) {
            Result res = db.execute("RETURN apoc.temporal.formatDuration( duration('P0M0DT4820.487660000S'), \"HH:mm:ss\" ) as output");

            assertEquals("01:20:20", res.next().get("output"));
        }
    }

    @Test
    public void shouldFormatDurationTemporalISO() throws Throwable
    {
        try (Transaction tx = db.beginTx() ) {
            Result res = db.execute("RETURN apoc.temporal.formatDuration( duration('P0M0DT4820.487660000S'), \"ISO_DATE_TIME\" ) as output");

            assertEquals("0000-01-01T01:20:20.48766", res.next().get("output"));
        }
    }

    @Test
    public void shouldFormatIsoDate() throws Throwable
    {
        try (Transaction tx = db.beginTx() ) {
            Result res = db.execute("RETURN apoc.temporal.format( date( { year: 2018, month: 12, day: 10 } ), 'ISO_DATE' ) as output");

            assertEquals("2018-12-10", res.next().get("output"));
        }
    }

    @Test
    public void shouldFormatIsoLocalDateTime() throws Throwable
    {
        try (Transaction tx = db.beginTx() ) {
            Result res = db.execute("RETURN apoc.temporal.format( localdatetime( { year: 2018, month: 12, day: 10, hour: 12, minute: 34, second: 56, nanosecond: 123456789 } ), 'ISO_LOCAL_DATE_TIME' ) as output");

            assertEquals("2018-12-10T12:34:56.123456789", res.next().get("output"));
        }
    }

    @Test
    public void shouldReturnTheDateWithDefault() throws Throwable
    {
        try (Transaction tx = db.beginTx() ) {
            Result res = db.execute("RETURN apoc.temporal.format( localdatetime( { year: 2018, month: 12, day: 10, hour: 12, minute: 34, second: 56, nanosecond: 123456789 } )) as output");

            assertEquals("2018-12-10", res.next().get("output"));
        }
    }

    @Test
    public void shouldReturnTheDateWithDefaultElastic() throws Throwable
    {
        try (Transaction tx = db.beginTx() ) {
            Result res = db.execute("RETURN apoc.temporal.format( localdatetime( { year: 2018, month: 12, day: 10, hour: 12, minute: 34, second: 56, nanosecond: 123456789 } ), 'DATE_HOUR_MINUTE_SECOND_FRACTION') as output");

            assertEquals("2018-12-10T12:34:56.123", res.next().get("output"));
        }
    }

    @Test
    public void shouldFormatIsoDateWeek() throws Throwable
    {
        try (Transaction tx = db.beginTx() ) {
            Result res = db.execute("RETURN apoc.temporal.format( date( { year: 2018, month: 12, day: 10 } ), 'date' ) as output");

            assertEquals("2018-12-10", res.next().get("output"));
        }
    }

    @Test
    public void shouldFormatIsoYear() throws Throwable
    {
        try (Transaction tx = db.beginTx() ) {
            Result res = db.execute("RETURN apoc.temporal.format( date( { year: 2018, month: 12, day: 10 } ), 'date' ) as output");

            assertEquals("2018-12-10", res.next().get("output"));
        }
    }

    @Test
    public void shouldFormatIsoOrdinalDate() throws Throwable
    {
        try (Transaction tx = db.beginTx() ) {
            Result res = db.execute("RETURN apoc.temporal.format( date( { year: 2018, month: 12, day: 10 } ), 'ordinal_date' ) as output");

            assertEquals("2018-344", res.next().get("output"));
        }
    }

    @Test
    public void shouldFormatIsoDateWeekError(){
    expected.expect(instanceOf(RuntimeException.class));
        try (Transaction tx = db.beginTx() ) {
            Result res = db.execute("RETURN apoc.temporal.format( date( { year: 2018, month: 12, day: 10 } ), 'WRONG_FORMAT' ) as output");

            assertEquals("2018-12-10", res.next().get("output"));
        }
    }

    @Test
    public void shouldFormatDurationIsoDateWeekError(){
        expected.expect(instanceOf(RuntimeException.class));
        try (Transaction tx = db.beginTx() ) {
            Result res = db.execute("RETURN apoc.temporal.formatDuration( date( { year: 2018, month: 12, day: 10 } ), 'wrongDuration' ) as output");

            assertEquals("2018-12-10", res.next().get("output"));
        }
    }


}
