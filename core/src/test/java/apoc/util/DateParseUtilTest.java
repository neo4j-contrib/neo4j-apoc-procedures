package apoc.util;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.junit.Test;
import org.neo4j.graphdb.QueryExecutionException;

import java.time.*;
import java.util.List;

import static apoc.util.DateParseUtil.dateParse;
import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testCall;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DateParseUtilTest {

    private String[] parseList = new String[]{"wrongPath", "dd-MM-yyyy", "dd/MM/yyyy", "yyyy/MM/dd'T'HH:mm:ss", "yyyy/dd/MM", "iso_zoned_date_time", "yyyy-MM-dd HH:mm", "HH mm"};

    @Test
    public void dateParseTest() {
        assertEquals(LocalDate.of(2018,01,01),dateParse("2018/01/01", LocalDate.class, parseList));
        assertEquals(ZonedDateTime.of(2011,01,01,12,0,0,53810000,ZoneOffset.of("+01:00")),dateParse("2011-01-01T12:00:00.05381+01:00", ZonedDateTime.class, parseList));
        assertEquals(LocalDateTime.of(2018,05,10,12,10),dateParse("2018-05-10 12:10", LocalDateTime.class, parseList));
        assertEquals(LocalTime.of(12,10),dateParse("12 10", LocalTime.class, parseList));
        assertEquals(OffsetTime.of(10,15,30,0,ZoneOffset.of("+01:00")),dateParse("10:15:30+01:00", OffsetTime.class, parseList));
    }

    @Test(expected = RuntimeException.class)
    public void dateParseErrorTest() throws Exception {
        try {
            dateParse("10/01/2010", LocalDateTime.class, parseList);
        } catch (Exception e) {
            assertTrue(e instanceof RuntimeException);
            assertEquals("Can't format the date with the pattern", e.getMessage());
            throw e;
        }
    }
}