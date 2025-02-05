package apoc.load.jdbc;

import apoc.util.Util;

import java.time.Instant;
import java.util.Calendar;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public abstract class AbstractJdbcTest {
    protected static final String MATCH_SQL_ANALYTICS = "MATCH (n:City) RETURN n.localdatetime AS localdatetime, n.localtime AS localtime, n.duration AS duration, n.date AS date, n.datetime AS datetime, n.time AS time, n.country AS country, n.name AS name, n.year AS year, n.population AS population";

    protected static java.sql.Date hireDate = new java.sql.Date(new Calendar.Builder().setDate(2017, 04, 25).build().getTimeInMillis());

    protected static java.sql.Timestamp effectiveFromDate = java.sql.Timestamp.from(Instant.parse("2016-06-22T17:10:25Z"));

    protected static java.sql.Time time = java.sql.Time.valueOf("15:37:00");

    protected static final String ANALYTICS_CYPHER_FILE = "dataset-analytics.cypher";

    public void assertResult(Map<String, Object> row) {
        Map<String, Object> expected = Util.map("NAME", "John", "SURNAME", null, "HIRE_DATE", hireDate.toLocalDate(), "EFFECTIVE_FROM_DATE",
                effectiveFromDate.toLocalDateTime(), "TEST_TIME", time.toLocalTime(), "NULL_DATE", null);
        assertEquals(expected, row.get("row"));
    }
    
    protected static void assertRowRank(Map<String, Object> row, Object expected) {
        var result = (Map) row.get("row");
        Object rank = result.get("rank");
        assertEquals(expected, rank);
    }
}
