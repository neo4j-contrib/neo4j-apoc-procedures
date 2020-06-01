package apoc.load;

import apoc.util.Util;

import java.time.Instant;
import java.util.Calendar;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public abstract class AbstractJdbcTest {

    protected static java.sql.Date hireDate = new java.sql.Date(new Calendar.Builder().setDate(2017, 04, 25).build().getTimeInMillis());

    protected static java.sql.Timestamp effectiveFromDate = java.sql.Timestamp.from(Instant.parse("2016-06-22T17:10:25Z"));

    protected static java.sql.Time time = java.sql.Time.valueOf("15:37:00");

    public void assertResult(Map<String, Object> row) {
        Map<String, Object> expected = Util.map("NAME", "John", "SURNAME", null, "HIRE_DATE", hireDate.toLocalDate(), "EFFECTIVE_FROM_DATE",
                effectiveFromDate.toLocalDateTime(), "TEST_TIME", time.toLocalTime(), "NULL_DATE", null);
        assertEquals(expected, row.get("row"));
    }
}
