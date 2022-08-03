package apoc.temporal;

import apoc.util.TestUtil;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertEquals;

public class TemporalProceduresTest
{
    @Rule public ExpectedException expected = ExpectedException.none();

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    @BeforeClass public static void setUp() throws Exception {
        TestUtil.registerProcedure(db, TemporalProcedures.class);
    }

    @Test
    public void shouldFormatDate() throws Throwable
    {
        String output = TestUtil.singleResultFirstColumn(db, "RETURN apoc.temporal.format( date( { year: 2018, month: 12, day: 10 } ), \"yyyy-MM-dd\" ) as output");
        assertEquals("2018-12-10", output);
    }

    @Test
    public void shouldFormatDateTime() throws Throwable
    {
        String output = TestUtil.singleResultFirstColumn(db, "RETURN apoc.temporal.format( datetime( { year: 2018, month: 12, day: 10, hour: 12, minute: 34, second: 56, nanosecond: 123456789 } ), \"yyyy-MM-dd'T'HH:mm:ss.SSSS\" ) as output");
        assertEquals("2018-12-10T12:34:56.1234", output);
    }

    @Test
    public void shouldFormatLocalDateTime() throws Throwable
    {
        String output = TestUtil.singleResultFirstColumn(db, "RETURN apoc.temporal.format( localdatetime( { year: 2018, month: 12, day: 10, hour: 12, minute: 34, second: 56, nanosecond: 123456789 } ), \"yyyy-MM-dd'T'HH:mm:ss.SSSS\" ) as output");
        assertEquals("2018-12-10T12:34:56.1234", output);
    }

    @Test
    public void shouldFormatTime() throws Throwable
    {
        String output = TestUtil.singleResultFirstColumn(db, "RETURN apoc.temporal.format( time( { hour: 12, minute: 34, second: 56, nanosecond: 123456789, timezone: 'GMT' } ), \"HH:mm:ss.SSSSZ\" ) as output");
        assertEquals("12:34:56.1234+0000", output);
    }

    @Test
    public void shouldFormatLocalTime() throws Throwable
    {
        String output = TestUtil.singleResultFirstColumn(db, "RETURN apoc.temporal.format( localtime( { hour: 12, minute: 34, second: 56, nanosecond: 123456789 } ), \"HH:mm:ss.SSSS\" ) as output");
        assertEquals("12:34:56.1234", output);
    }

    @Test
    public void shouldFormatDuration() throws Throwable
    {
        String output = TestUtil.singleResultFirstColumn(db, "RETURN apoc.temporal.format( duration('P0M0DT4820.487660000S'), \"HH:mm:ss.SSSS\" ) as output");
        assertEquals("01:20:20.4876", output);
    }

    @Test
    public void shouldFormatDurationTemporal() throws Throwable
    {
        String output = TestUtil.singleResultFirstColumn(db, "RETURN apoc.temporal.formatDuration( duration('P0M0DT4820.487660000S'), \"HH:mm:ss\" ) as output");
        assertEquals("01:20:20", output);
    }

    @Test
    public void shouldFormatDurationTemporalISO() throws Throwable
    {
        String output = TestUtil.singleResultFirstColumn(db, "RETURN apoc.temporal.formatDuration( duration('P0M0DT4820.487660000S'), \"ISO_DATE_TIME\" ) as output");
        assertEquals("0000-01-01T01:20:20.48766", output);
    }

    @Test
    public void shouldFormatIsoDate() throws Throwable
    {
        String output = TestUtil.singleResultFirstColumn(db, "RETURN apoc.temporal.format( date( { year: 2018, month: 12, day: 10 } ), 'ISO_DATE' ) as output");
        assertEquals("2018-12-10", output);
    }

    @Test
    public void shouldFormatIsoLocalDateTime() throws Throwable
    {
        String output = TestUtil.singleResultFirstColumn(db, "RETURN apoc.temporal.format( localdatetime( { year: 2018, month: 12, day: 10, hour: 12, minute: 34, second: 56, nanosecond: 123456789 } ), 'ISO_LOCAL_DATE_TIME' ) as output");
        assertEquals("2018-12-10T12:34:56.123456789", output);
    }

    @Test
    public void shouldReturnTheDateWithDefault() throws Throwable
    {
        String output = TestUtil.singleResultFirstColumn(db, "RETURN apoc.temporal.format( localdatetime( { year: 2018, month: 12, day: 10, hour: 12, minute: 34, second: 56, nanosecond: 123456789 } )) as output");
        assertEquals("2018-12-10", output);
    }

    @Test
    public void shouldReturnTheDateWithDefaultElastic() throws Throwable
    {
        String output = TestUtil.singleResultFirstColumn(db, "RETURN apoc.temporal.format( localdatetime( { year: 2018, month: 12, day: 10, hour: 12, minute: 34, second: 56, nanosecond: 123456789 } ), 'DATE_HOUR_MINUTE_SECOND_FRACTION') as output");
        assertEquals("2018-12-10T12:34:56.123", output);
    }

    @Test
    public void shouldFormatIsoDateWeek() throws Throwable
    {
        String output = TestUtil.singleResultFirstColumn(db, "RETURN apoc.temporal.format( date( { year: 2018, month: 12, day: 10 } ), 'date' ) as output");
        assertEquals("2018-12-10", output);
    }

    @Test
    public void shouldFormatIsoYear() throws Throwable
    {
        String output = TestUtil.singleResultFirstColumn(db, "RETURN apoc.temporal.format( date( { year: 2018, month: 12, day: 10 } ), 'date' ) as output");
        assertEquals("2018-12-10", output);
    }

    @Test
    public void shouldFormatIsoOrdinalDate() throws Throwable
    {
        String output = TestUtil.singleResultFirstColumn(db, "RETURN apoc.temporal.format( date( { year: 2018, month: 12, day: 10 } ), 'ordinal_date' ) as output");
        assertEquals("2018-344", output);
    }

    @Test
    public void shouldFormatIsoDateWeekError(){
    expected.expect(instanceOf(RuntimeException.class));
        String output = TestUtil.singleResultFirstColumn(db, "RETURN apoc.temporal.format( date( { year: 2018, month: 12, day: 10 } ), 'WRONG_FORMAT' ) as output");
        assertEquals("2018-12-10", output);
    }

    @Test
    public void shouldFormatDurationIsoDateWeekError(){
        expected.expect(instanceOf(RuntimeException.class));
        String output = TestUtil.singleResultFirstColumn(db, "RETURN apoc.temporal.formatDuration( date( { year: 2018, month: 12, day: 10 } ), 'wrongDuration' ) as output");
        assertEquals("2018-12-10", output);
    }


}
