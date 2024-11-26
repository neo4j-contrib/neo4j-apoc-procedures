package apoc.temporal;

import apoc.Extended;
import apoc.util.TestUtil;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;
import org.neo4j.values.storable.DateTimeValue;
import org.neo4j.values.storable.DateValue;
import org.neo4j.values.storable.LocalDateTimeValue;
import org.neo4j.values.storable.LocalTimeValue;
import org.neo4j.values.storable.TimeValue;

import java.time.ZoneId;
import java.util.Map;

import static apoc.temporal.TemporalExtended.ACCEPT_ADJACENT_KEY;
import static apoc.util.TestUtil.singleResultFirstColumn;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@Extended
public class TemporalExtendedTest {

    public static final String RETURN_OVERLAP = "RETURN apoc.temporal.overlap($start1, $end1, $start2, $end2, $conf)";
    
    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    @BeforeClass
    public static void setUp() {
        TestUtil.registerProcedure(db, TemporalExtended.class);
    }

    @Test
    public void testOverlapDates() {
        Map<String, Object> params = Map.of("start1", DateValue.parse("1999"),
                "end1", DateValue.parse("2000"),
                "start2", DateValue.parse("2000"),
                "end2", DateValue.parse("2001"),
                "conf", Map.of());
        
        boolean output = singleResultFirstColumn(db, RETURN_OVERLAP, params);
        assertFalse(output);
    }
    
    @Test
    public void testOverlapDatesWithConfigAcceptAdjacentSpansConf() {
        Map<String, Object> params = Map.of("start1", DateValue.parse("1999"),
                "end1", DateValue.parse("2000"),
                "start2", DateValue.parse("2000"),
                "end2", DateValue.parse("2001"),
                "conf", Map.of(ACCEPT_ADJACENT_KEY, true));
        
        boolean output = singleResultFirstColumn(db, RETURN_OVERLAP, params);
        assertTrue(output);
    }

    @Test
    public void testOverlapWithDatetime() {
        Map<String, Object> params = Map.of("start1", DateTimeValue.parse("1999", ZoneId::systemDefault),
                "end1", DateTimeValue.parse("2000", ZoneId::systemDefault),
                "start2", DateTimeValue.parse("2000", ZoneId::systemDefault),
                "end2", DateTimeValue.parse("2001", ZoneId::systemDefault),
                "conf", Map.of());

        boolean output = singleResultFirstColumn(db, RETURN_OVERLAP, params);
        assertFalse(output);
    }

    @Test
    public void testOverlapWithDatetimeAndAcceptAdjacentSpansConf() {
        Map<String, Object> params = Map.of("start1", DateTimeValue.parse("1999", ZoneId::systemDefault),
                "end1", DateTimeValue.parse("2000", ZoneId::systemDefault),
                "start2", DateTimeValue.parse("2000", ZoneId::systemDefault),
                "end2", DateTimeValue.parse("2001", ZoneId::systemDefault),
                "conf", Map.of(ACCEPT_ADJACENT_KEY, true));

        boolean output = singleResultFirstColumn(db, RETURN_OVERLAP, params);
        assertTrue(output);
    }

    @Test
    public void testOverlapWithTime() {
                Map<String, Object> params = Map.of("start1", TimeValue.parse("00:01", ZoneId::systemDefault),
                "end1", TimeValue.parse("01:01", ZoneId::systemDefault),
                "start2", TimeValue.parse("00:00", ZoneId::systemDefault),
                "end2", TimeValue.parse("00:02", ZoneId::systemDefault),
                "conf", Map.of());

        boolean output = singleResultFirstColumn(db, RETURN_OVERLAP, params);
        assertTrue(output);
    }

    @Test
    public void testOverlapWithLocalTime() {
                Map<String, Object> params = Map.of("start1", LocalTimeValue.parse("00:01"),
                "end1", LocalTimeValue.parse("01:01"),
                "start2", LocalTimeValue.parse("00:00"),
                "end2", LocalTimeValue.parse("00:02"),
                "conf", Map.of(ACCEPT_ADJACENT_KEY, true));

        boolean output = singleResultFirstColumn(db, RETURN_OVERLAP, params);
        assertTrue(output);
    }

    @Test
    public void testOverlapWithLocalDateTime() {
                Map<String, Object> params = Map.of("start1", LocalDateTimeValue.parse("1999"),
                "end1", LocalDateTimeValue.parse("2000"),
                "start2", LocalDateTimeValue.parse("2000"),
                "end2", LocalDateTimeValue.parse("2001"),
                "conf", Map.of());

        boolean output = singleResultFirstColumn(db, RETURN_OVERLAP, params);
        assertFalse(output);
    }

    /**
     * In this test case the 2 ranges have different types (i.e. `date` and `time`),
     * and we just return `null`, 
     * to be consistent with Cypher's behavior (e.g., `return date("1999") > time("19:00")` has result `null` )
     */
    @Test
    public void testOverlapWithWrongTypes() {
        Map<String, Object> params = Map.of("start1", DateValue.parse("1999"),
                "end1", DateValue.parse("2000"),
                "start2", LocalTimeValue.parse("19:00"),
                "end2", LocalTimeValue.parse("20:00"),
                "conf", Map.of());

        Object output = singleResultFirstColumn(db, RETURN_OVERLAP, params);
        assertNull(output);
    }

}
