package apoc.load;

import apoc.load.jdbc.AbstractJdbcTest;
import apoc.load.jdbc.Analytics;
import apoc.load.jdbc.Jdbc;
import apoc.util.s3.MySQLContainerExtension;
import apoc.util.TestUtil;
import apoc.util.Util;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;

import static apoc.load.jdbc.Analytics.PROVIDER_CONF_KEY;
import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testResult;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(Enclosed.class)
public class MySQLJdbcTest extends AbstractJdbcTest {
    
    public static class MySQLJdbcLatestVersionTest {
        
        @ClassRule
        public static MySQLContainerExtension mysql = new MySQLContainerExtension("mysql:8.0.31");

        @ClassRule
        public static DbmsRule db = new ImpermanentDbmsRule();
        
        @BeforeClass
        public static void setUpContainer() throws Exception {
            mysql.start();
            TestUtil.registerProcedure(db, Jdbc.class, Analytics.class);
            createAnalyticsDataset(db);
        }

        @AfterClass
        public static void tearDown() {
            mysql.stop();
            db.shutdown();
        }

        @Test
        public void testLoadJdbc() {
            MySQLJdbcTest.testLoadJdbc(db, mysql);
        }

        @Test
        public void testIssue3496() {
            MySQLJdbcTest.testIssue3496(db, mysql);
        }

        @Test
        public void testLoadJdbcAnalytics() {

            String sql = """
                SELECT
                    country,
                    name,
                    year,
                    date,
                    time,
                    datetime,
                    localtime,
                    localdatetime,
                    duration,
                    population,
                    blobData,
                    RANK() OVER (PARTITION BY country ORDER BY year DESC) AS 'rank'
                FROM %s
                ORDER BY country, name;
               """.formatted(Analytics.TABLE_NAME_DEFAULT_CONF_KEY);
            List<String> expectedResults = List.of("3", "1", "2", "1", "5", "3", "5", "1", "3");
            testResult(db, "CALL apoc.jdbc.analytics($queryCypher, $url, $sql, [], $config)",
                    map(
                            "queryCypher", MATCH_ANALYTICS_QUERY,
                            "sql", sql,
                            "url", mysql.getJdbcUrl(),
                            "config", map(PROVIDER_CONF_KEY, Analytics.Provider.MYSQL.name())
                    ),
                    r -> commonAnalyticsAssertions(r, expectedResults));
        }

        @Test
        public void testLoadJdbcAnalyticsWindow() {

            String sql = """
                SELECT
                    country,
                    name,
                    year,
                    date,
                    time,
                    datetime,
                    localtime,
                    localdatetime,
                    duration,
                    population,
                    blobData,
                    ROW_NUMBER() OVER (PARTITION BY country ORDER BY year DESC) AS 'rank'
                FROM %s
                ORDER BY country, name;
               """.formatted(Analytics.TABLE_NAME_DEFAULT_CONF_KEY);

            List<String> expectedResults = List.of("3", "1", "2", "1", "6", "4", "5", "2", "3");
            testResult(db, "CALL apoc.jdbc.analytics($queryCypher, $url, $sql, [], $config)",
                    map(
                            "queryCypher", MATCH_ANALYTICS_QUERY,
                            "sql", sql,
                            "url", mysql.getJdbcUrl(),
                            "config", map(PROVIDER_CONF_KEY, Analytics.Provider.MYSQL.name())
                    ),
                    r -> commonAnalyticsAssertions(r, expectedResults));
        }

        private static void commonAnalyticsAssertions(Result r, List<String> expectedResults) {
            expectedResults.forEach(expected -> {
                Map<String, Object> actual = r.next();
                assertRowRank(actual, expected);
            });

            assertFalse(r.hasNext());
        }
    }
    
    public static class MySQLJdbcFiveVersionTest {
        
        @ClassRule
        public static MySQLContainerExtension mysql = new MySQLContainerExtension("mysql:5.7");

        @ClassRule
        public static DbmsRule db = new ImpermanentDbmsRule();

        @BeforeClass
        public static void setUpContainer() {
            mysql.start();
            TestUtil.registerProcedure(db, Jdbc.class);
        }

        @AfterClass
        public static void tearDown() {
            mysql.stop();
            db.shutdown();
        }

        @Test
        public void testLoadJdbc() {
            MySQLJdbcTest.testLoadJdbc(db, mysql);
        }

        @Test
        public void testIssue3496() {
            MySQLJdbcTest.testIssue3496(db, mysql);
        }
    }

    private static void testLoadJdbc(DbmsRule db, MySQLContainerExtension mysql) {
        // with the config {timezone: 'UTC'} and `preserveInstants=true&connectionTimeZone=SERVER` to make the result deterministic,
        // since `TIMESTAMP` values are automatically converted from the session time zone to UTC for storage, and vice versa.
        testCall(db, "CALL apoc.load.jdbc($url, $table, [], {timezone: 'UTC'})",
                Util.map(
                        "url", mysql.getJdbcUrl() + "&preserveInstants=true&connectionTimeZone=SERVER",
                        "table", "country"),
                row -> {
                    Map<String, Object> expected = Util.map(
                            "Code", "NLD",
                            "Name", "Netherlands",
                            "Continent", "Europe",
                            "Region", "Western Europe",
                            "SurfaceArea", 41526f,
                            "IndepYear", 1581,
                            "Population", 15864000,
                            "LifeExpectancy", 78.3f,
                            "GNP", 371362f,
                            "GNPOld", 360478f,
                            "LocalName", "Nederland",
                            "GovernmentForm", "Constitutional Monarchy",
                            "HeadOfState", "Beatrix",
                            "Capital", 5,
                            "Code2", "NL",
                            "myTime", LocalTime.of(1, 0, 0),
                            "myTimeStamp", ZonedDateTime.parse("2003-01-01T01:00Z"),
                            "myDate", LocalDate.parse("2003-01-01"),
                            "myYear", LocalDate.parse("2003-01-01")
                    );
                    Map actual = (Map) row.get("row");
                    Object myDateTime = actual.remove("myDateTime");
                    assertTrue(myDateTime instanceof LocalDateTime);
                    assertEquals(expected, actual);
                });
    }

    private static void testIssue3496(DbmsRule db, MySQLContainerExtension mysql) {
        testCall(db, "CALL apoc.load.jdbc($url,'SELECT DATE(NOW()), NOW(), CURDATE(), CURTIME(), UTC_DATE(), UTC_TIME(), UTC_TIMESTAMP(), DATE(UTC_TIMESTAMP());')", 
                Util.map("url", mysql.getJdbcUrl()),
                r -> {
                    Map row = (Map) r.get("row");
                    assertEquals(8, row.size());
                    
                    assertTrue(row.get("UTC_DATE()") instanceof LocalDate);
                    assertTrue(row.get("CURDATE()") instanceof LocalDate);
                    
                    assertTrue(row.get("UTC_TIMESTAMP()") instanceof LocalDateTime);
                    assertTrue(row.get("NOW()") instanceof LocalDateTime);
                    assertTrue(row.get("DATE(UTC_TIMESTAMP())") instanceof LocalDate);
                    assertTrue(row.get("DATE(NOW())") instanceof LocalDate);
                    
                    assertTrue(row.get("CURTIME()") instanceof LocalTime);
                    assertTrue(row.get("UTC_TIME()") instanceof LocalTime);
                });
    }
}
