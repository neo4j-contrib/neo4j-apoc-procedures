package apoc.load.jdbc;

import apoc.periodic.Periodic;
import apoc.util.MapUtil;
import apoc.util.TestUtil;
import apoc.util.Util;
import apoc.util.collection.Iterators;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.neo4j.graphdb.Result;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;
import org.neo4j.values.storable.DateValue;
import org.neo4j.values.storable.LocalDateTimeValue;
import org.neo4j.values.storable.LocalTimeValue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.time.*;
import java.util.Map;

import static apoc.ApocConfig.apocConfig;
import static apoc.load.jdbc.Analytics.*;
import static apoc.load.util.JdbcUtil.KEY_NOT_FOUND_MESSAGE;
import static apoc.util.ExtendedTestUtil.assertFails;
import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class DuckDBJdbcTest extends AbstractJdbcTest {

    public String JDBC_DUCKDB = null;
    
    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();

    private Connection conn;

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Before
    public void setUp() throws Exception {
        JDBC_DUCKDB = "jdbc:duckdb:" + temporaryFolder.newFolder() + "/testDB";
        apocConfig().setProperty("apoc.jdbc.duckdb.url", JDBC_DUCKDB);
        apocConfig().setProperty("apoc.jdbc.test.sql","SELECT * FROM PERSON");
        apocConfig().setProperty("apoc.jdbc.testparams.sql","SELECT * FROM PERSON WHERE NAME = ?");
        TestUtil.registerProcedure(db, Jdbc.class, Periodic.class, Analytics.class);
        
        conn = DriverManager.getConnection(JDBC_DUCKDB);
        createPersonTableAndData(conn);

        createAnalyticsDataset(db);
    }

    @After
    public void tearDown() throws SQLException {
        conn.close();
    }
    
    @Test
    public void testLoadJdbcAnalyticsWithPivotOnAndAppendMode() throws SQLException {
        String customTable = "table_append";
        
        Statement statement = conn.createStatement();
        String createTableSql = String.format("CREATE TABLE %s (country VARCHAR, name VARCHAR, population DOUBLE, year INTEGER)", customTable);
        statement.execute(createTableSql);
        String insertSql = String.format("INSERT INTO %s VALUES ('FG','Zapponeta',1005,2000), ('FG','Zapponeta',1065,2010)", customTable);
        statement.execute(insertSql);
        
        String cypher = "MATCH (n:City) RETURN n.country AS country, n.name AS name, n.year AS year, n.population AS population";

        String analyticsSql = String.format("""
                PIVOT %s
                ON year
                USING sum(population)
                ORDER by name
                """, 
                customTable);

        testResult(db, "CALL apoc.jdbc.analytics($queryCypher, $url, $sql, [], $config)",
                map(
                        "queryCypher", cypher,
                        "sql", analyticsSql,
                        "url", JDBC_DUCKDB,
                        "config", map(
                                TABLE_NAME_CONF_KEY, customTable, 
                                WRITE_MODE_CONF_KEY, Analytics.WriteMode.APPEND.toString()
                        )
                ),
                r -> {
                    String rowKey = "row";
                    String nameKey = "name";
                    String countryKey = "country";

                    pivotOnAssertions(r, rowKey, nameKey, countryKey);
                    Map<String, Object> row = r.next();
                    var result = (Map) row.get(rowKey);
                    assertEquals("Zapponeta", result.get(nameKey));
                    assertEquals("FG", result.get(countryKey));
                    assertEquals(1005.0, result.get("2000"));
                    assertEquals(1065.0, result.get("2010"));
                    assertNull(result.get("2020"));
                    
                    assertFalse(r.hasNext());
                });

        String dropSql = String.format("DROP TABLE %s", customTable);
        statement.execute(dropSql);
    }
    
    @Test
    public void testLoadJdbcAnalyticsWithWrongBatchSize() {
        assertFails(db, "CALL apoc.jdbc.analytics('match (n) return n', $url, 'SELECT country FROM tableName', [], $conf)",
                map("url", JDBC_DUCKDB, "conf", map(BATCH_SIZE_CONF_KEY, -7)),
                WRONG_BATCH_SIZE_ERR
        );
    }
    
    @Test
    public void testLoadJdbcAnalyticsWithEmptySQL() {
        assertFails(db, "CALL apoc.jdbc.analytics('match (n) return n', $url, '')", 
                map("url", JDBC_DUCKDB),
                EMPTY_SQL_QUERY_ERROR
        );
    }
    
    @Test
    public void testLoadJdbcAnalyticsWithEmptyCypher() {
        assertFails(db, "CALL apoc.jdbc.analytics('', $url, 'SELECT country FROM tableName')",
                map("url", JDBC_DUCKDB),
                EMPTY_NEO4J_QUERY_ERROR
        );
    }
    
    @Test
    public void testLoadJdbcAnalyticsWithEmptyUrl() {
        assertFails(db, "CALL apoc.jdbc.analytics('match (n) return n', '', 'SELECT country FROM tableName')",
                map(),
                String.format(KEY_NOT_FOUND_MESSAGE, "")
        );
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
                RANK() OVER (PARTITION BY country ORDER BY year DESC) AS rank
            FROM %s
            ORDER BY rank, country, name, date;
            """
            .formatted(Analytics.TABLE_NAME_DEFAULT_CONF_KEY);
        testResult(db, "CALL apoc.jdbc.analytics($queryCypher, $url, $sql)",
                map(
                        "queryCypher", MATCH_ANALYTICS_QUERY,
                        "sql", sql,
                        "url", JDBC_DUCKDB
                ),
                r -> {
                    Map<String, Object> row = r.next();
                    String rowKey = "row";
                    String rankKey = "rank";
                    String nameKey = "name";
                    String countryKey = "country";
                    var result = (Map) row.get(rowKey);
                    var rank = (long) result.get(rankKey);
                    assertEquals(1, rank);
                    assertEquals("Amsterdam", result.get(nameKey));
                    assertEquals("NL", result.get(countryKey));
                    assertEquals(LocalDateTimeValue.parse("1989").asObjectCopy(), result.get("datetime"));
                    LocalTime timeExpected = LocalTimeValue.parse("11:30").asObjectCopy();
                    assertEquals(timeExpected, result.get("time"));
                    assertEquals(DateValue.parse("1989").asObjectCopy(), result.get("date"));
                    assertEquals(LocalDateTimeValue.parse("1989").asObjectCopy(), result.get("localdatetime"));
                    assertEquals(timeExpected, result.get("localtime"));
                    assertEquals("PT45S", result.get("duration"));
                    assertTrue(result.get("blobdata") instanceof byte[]);

                    row = r.next();
                    result = (Map) row.get(rowKey);
                    rank = (long) result.get(rankKey);
                    assertEquals(1, rank);
                    assertEquals("New York City", result.get(nameKey));
                    assertEquals("US", result.get(countryKey));
                    assertEquals(LocalDateTimeValue.parse("2009").asObjectCopy(), result.get("datetime"));
                    assertEquals(timeExpected, result.get("time"));
                    assertEquals(DateValue.parse("2009").asObjectCopy(), result.get("date"));
                    assertEquals(LocalDateTimeValue.parse("2009").asObjectCopy(), result.get("localdatetime"));
                    assertEquals(timeExpected, result.get("localtime"));
                    assertEquals("PT45S", result.get("duration"));
                    assertTrue(result.get("blobdata") instanceof byte[]);

                    row = r.next();
                    result = (Map) row.get(rowKey);
                    rank = (long) result.get(rankKey);
                    assertEquals(1, rank);
                    assertEquals("Seattle", result.get(nameKey));
                    assertEquals("US", result.get(countryKey));
                    assertEquals(LocalDateTimeValue.parse("2009").asObjectCopy(), result.get("datetime"));
                    assertEquals(timeExpected, result.get("time"));
                    assertEquals(DateValue.parse("2009").asObjectCopy(), result.get("date"));
                    assertEquals(LocalDateTimeValue.parse("2009").asObjectCopy(), result.get("localdatetime"));
                    assertEquals(timeExpected, result.get("localtime"));
                    assertEquals("PT45S", result.get("duration"));
                    assertTrue(result.get("blobdata") instanceof byte[]);

                    row = r.next();
                    result = (Map) row.get(rowKey);
                    rank = (long) result.get(rankKey);
                    assertEquals(2, rank);
                    assertEquals("Amsterdam", result.get(nameKey));
                    assertEquals("NL", result.get(countryKey));
                    assertEquals(LocalDateTimeValue.parse("1989").asObjectCopy(), result.get("datetime"));
                    assertEquals(timeExpected, result.get("time"));
                    assertEquals(DateValue.parse("1989").asObjectCopy(), result.get("date"));
                    assertEquals(LocalDateTimeValue.parse("1989").asObjectCopy(), result.get("localdatetime"));
                    assertEquals(timeExpected, result.get("localtime"));
                    assertEquals("PT45S", result.get("duration"));
                    assertTrue(result.get("blobdata") instanceof byte[]);

                    row = r.next();
                    result = (Map) row.get(rowKey);
                    rank = (long) result.get(rankKey);
                    assertEquals(3, rank);
                    assertEquals("Amsterdam", result.get(nameKey));
                    assertEquals("NL", result.get(countryKey));
                    assertEquals(LocalDateTimeValue.parse("1999").asObjectCopy(), result.get("datetime"));
                    assertEquals(timeExpected, result.get("time"));
                    assertEquals(DateValue.parse("1999").asObjectCopy(), result.get("date"));
                    assertEquals(LocalDateTimeValue.parse("1999").asObjectCopy(), result.get("localdatetime"));
                    assertEquals(timeExpected, result.get("localtime"));
                    assertEquals("PT45S", result.get("duration"));
                    assertTrue(result.get("blobdata") instanceof byte[]);

                    row = r.next();
                    result = (Map) row.get(rowKey);
                    rank = (long) result.get(rankKey);
                    assertEquals(3, rank);
                    assertEquals("New York City", result.get(nameKey));
                    assertEquals("US", result.get(countryKey));
                    assertEquals(LocalDateTimeValue.parse("2009").asObjectCopy(), result.get("datetime"));
                    assertEquals(timeExpected, result.get("time"));
                    assertEquals(DateValue.parse("2009").asObjectCopy(), result.get("date"));
                    assertEquals(LocalDateTimeValue.parse("2009").asObjectCopy(), result.get("localdatetime"));
                    assertEquals(timeExpected, result.get("localtime"));
                    assertEquals("PT45S", result.get("duration"));
                    assertTrue(result.get("blobdata") instanceof byte[]);

                    row = r.next();
                    result = (Map) row.get(rowKey);
                    rank = (long) result.get(rankKey);
                    assertEquals(3, rank);
                    assertEquals("Seattle", result.get(nameKey));
                    assertEquals("US", result.get(countryKey));
                    assertEquals(LocalDateTimeValue.parse("2019").asObjectCopy(), result.get("datetime"));
                    assertEquals(timeExpected, result.get("time"));
                    assertEquals(DateValue.parse("2019").asObjectCopy(), result.get("date"));
                    assertEquals(LocalDateTimeValue.parse("2019").asObjectCopy(), result.get("localdatetime"));
                    assertEquals(timeExpected, result.get("localtime"));
                    assertEquals("PT45S", result.get("duration"));
                    assertTrue(result.get("blobdata") instanceof byte[]);

                    row = r.next();
                    result = (Map) row.get(rowKey);
                    rank = (long) result.get(rankKey);
                    assertEquals(5, rank);
                    assertEquals("New York City", result.get(nameKey));
                    assertEquals("US", result.get(countryKey));
                    assertEquals(LocalDateTimeValue.parse("2009").asObjectCopy(), result.get("datetime"));
                    assertEquals(timeExpected, result.get("time"));
                    assertEquals(DateValue.parse("2009").asObjectCopy(), result.get("date"));
                    assertEquals(LocalDateTimeValue.parse("2009").asObjectCopy(), result.get("localdatetime"));
                    assertEquals(timeExpected, result.get("localtime"));
                    assertEquals("PT45S", result.get("duration"));
                    assertTrue(result.get("blobdata") instanceof byte[]);

                    row = r.next();
                    result = (Map) row.get(rowKey);
                    rank = (long) result.get(rankKey);
                    assertEquals(5, rank);
                    assertEquals("Seattle", result.get(nameKey));
                    assertEquals("US", result.get(countryKey));
                    assertEquals(LocalDateTimeValue.parse("2019").asObjectCopy(), result.get("datetime"));
                    assertEquals(timeExpected, result.get("time"));
                    assertEquals(DateValue.parse("2019").asObjectCopy(), result.get("date"));
                    assertEquals(LocalDateTimeValue.parse("2019").asObjectCopy(), result.get("localdatetime"));
                    assertEquals(timeExpected, result.get("localtime"));
                    assertEquals("PT45S", result.get("duration"));
                    assertTrue(result.get("blobdata") instanceof byte[]);
                    
                    assertFalse(r.hasNext());
                });
    }

    @Test
    public void testLoadJdbcAnalyticsDuckDBWindowAndPivot() {

        String sql = """
                WITH ranked_data AS (
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
                        ROW_NUMBER() OVER (PARTITION BY country ORDER BY year DESC) AS rank
                    FROM %s
                    ORDER BY rank, country, name, date
                )
                SELECT *
                FROM ranked_data
                PIVOT (
                    sum(population)
                    FOR country IN ('NL', 'US')
                    GROUP BY year
                )
                """.formatted(Analytics.TABLE_NAME_DEFAULT_CONF_KEY);;

        testResult(db, "CALL apoc.jdbc.analytics($queryCypher, $url, $sql)",
                map(
                        "queryCypher", MATCH_ANALYTICS_QUERY,
                        "sql", sql,
                        "url", JDBC_DUCKDB
                ),
                r -> {
                    Map<String, Object> row = r.next();
                    String rowKey = "row";
                    String yearKey = "year";
                    String nlKey = "NL";
                    String usKey = "US";
                    
                    var result = (Map) row.get(rowKey);
                    assertEquals(2000L, result.get(yearKey));
                    assertEquals(1005.0, result.get(nlKey));
                    assertEquals(8579.9, (double) result.get(usKey), 0.1);

                    row = r.next();
                    result = (Map) row.get(rowKey);
                    assertEquals(2010L, result.get(yearKey));
                    assertEquals(1065.1, result.get(nlKey));
                    assertEquals(8784.1, (double) result.get(usKey), 0.1);
 
                    row = r.next();
                    result = (Map) row.get(rowKey);
                    assertEquals(2020L, result.get(yearKey));
                    assertEquals(1158.2, result.get(nlKey));
                    assertEquals(9511.3, (double) result.get(usKey), 0.1);

                    assertFalse(r.hasNext());
                });
    }

    @Test
    public void testLoadJdbcAnalyticsDuckDBPivotOnAndCustomTableName() {
        testLoadJdbcAnalyticsDuckDBPivotOnCommon(JDBC_DUCKDB);
    }
    
    @Test
    public void testLoadJdbcAnalyticsDuckDBPivotOnAndInMemoryDB() {
        testLoadJdbcAnalyticsDuckDBPivotOnCommon("jdbc:duckdb:");
    }

    private void testLoadJdbcAnalyticsDuckDBPivotOnCommon(String jdbcUrl) {
        String cypher = "MATCH (n:City) RETURN n.country AS country, n.name AS name, n.year AS year, n.population AS population";
        String customTable = "cities";

        String sql = """
                PIVOT %s
                ON year
                USING sum(population)
                ORDER by name, country
                """.formatted(customTable);

        testResult(db, "CALL apoc.jdbc.analytics($queryCypher, $url, $sql, [], $config)",
                map(
                        "queryCypher", cypher,
                        "sql", sql,
                        "url", jdbcUrl,
                        "config", map(Analytics.TABLE_NAME_CONF_KEY, customTable)
                ),
                r -> {
                    
                    String rowKey = "row";
                    String nameKey = "name";
                    String countryKey = "country";
                    pivotOnAssertions(r, rowKey, nameKey, countryKey);

                    assertFalse(r.hasNext());
                });
    }

    private static void pivotOnAssertions(Result r, String rowKey, String nameKey, String countryKey) {
        Map<String, Object> row = r.next();
        var result = (Map) row.get(rowKey);
        assertEquals("Amsterdam", result.get(nameKey));
        assertEquals("NL", result.get(countryKey));
        assertEquals(1005.0, result.get("2000"));
        assertEquals(1065.1, result.get("2010"));
        assertEquals(1158.2, result.get("2020"));

        row = r.next();
        result = (Map) row.get(rowKey);
        assertEquals("New York City", result.get(nameKey));
        assertEquals("US", result.get(countryKey));
        assertEquals(8015.6, result.get("2000"));
        assertEquals(8175.7, result.get("2010"));
        assertEquals(8772.8, result.get("2020"));

        row = r.next();
        result = (Map) row.get(rowKey);
        assertEquals("Seattle", result.get(nameKey));
        assertEquals("US", result.get(countryKey));
        assertEquals(564.3, result.get("2000"));
        assertEquals(608.4, result.get("2010"));
        assertEquals(738.5, result.get("2020"));
    }

    @Test
    public void testLoadJdbc() {
        testCall(db, "CALL apoc.load.jdbc($url,'PERSON')",
                map("url", JDBC_DUCKDB),
                this::assertResult);
    }

    @Test
    public void testLoadJdbcWithFetchSize() {
        testCall(db, "CALL apoc.load.jdbc($url,'PERSON', null, {fetchSize: 100})",
                map("url", JDBC_DUCKDB),
                this::assertResult);
    }

    @Test
    public void testLoadJdbcSelect() {
        testCall(db, "CALL apoc.load.jdbc($url,'SELECT * FROM PERSON')",
                map("url", JDBC_DUCKDB),
                this::assertResult);
    }
    
    @Test
    public void testLoadJdbcSelectColumnNames() {
        Map<String, Object> expected = map("NAME", "John",
                "DATE", AbstractJdbcTest.hireDate.toLocalDate());
        testCall(db, "CALL apoc.load.jdbc($url,'SELECT NAME, HIRE_DATE AS DATE FROM PERSON')",
                map("url", JDBC_DUCKDB),
                (row) -> assertEquals(expected, row.get("row")));
    }

    @Test
    public void testLoadJdbcParams() {
        testCall(db, "CALL apoc.load.jdbc($url,'SELECT * FROM PERSON WHERE NAME = ?',['John'])", //  YIELD row RETURN row
                map("url", JDBC_DUCKDB),
                this::assertResult);
    }

    @Test
    public void testLoadJdbcParamsWithConfigLocalDateTime() {
        testCall(db, "CALL apoc.load.jdbc($url,'SELECT * FROM PERSON WHERE NAME = ?',['John'])",
                map("url", JDBC_DUCKDB),
                this::assertResult);

        ZoneId asiaTokio = ZoneId.of("Asia/Tokyo");

        testCall(db, "CALL apoc.load.jdbc($url,'SELECT * FROM PERSON WHERE NAME = ?',['John'], $config)",
                map("url", JDBC_DUCKDB,
                        "config", map("timezone", asiaTokio.toString())),
                (row) -> {
                    Map<String, Object> expected = MapUtil.map("NAME", "John", "SURNAME", null,
                            "HIRE_DATE", AbstractJdbcTest.hireDate.toLocalDate(),
                            "EFFECTIVE_FROM_DATE", AbstractJdbcTest.effectiveFromDate.toInstant().atZone(asiaTokio).toOffsetDateTime().toZonedDateTime(),
                            "TEST_TIME", AbstractJdbcTest.time.toLocalTime(),
                            "NULL_DATE", null);
                    Map<String, Object> rowColumn = (Map<String, Object>) row.get("row");

                    expected.keySet().forEach( k -> {
                        // assertEquals(expected.get(k), rowColumn.get(k));
                    });
                    assertEquals(expected, rowColumn);
                }

        );
    }

    @Test
    public void testLoadJdbcParamsWithWrongTimezoneValue() {
        assertFails(db,"CALL apoc.load.jdbc('jdbc:duckdb:testDB','SELECT * FROM PERSON WHERE NAME = ?',['John'], {timezone: $timezone})",
                map("timezone", "Italy/Pescara"),
                "Failed to invoke procedure `apoc.load.jdbc`: Caused by: java.lang.IllegalArgumentException: The timezone field contains an error: Unknown time-zone ID: Italy/Pescara"
        );
    }

    @Test
    public void testLoadJdbcKey() {
        testCall(db, "CALL apoc.load.jdbc('duckdb','PERSON')",
                this::assertResult);
    }

    @Test
    public void testLoadJdbcSqlAlias() {
        testCall(db, "CALL apoc.load.jdbc('duckdb','test')",
                this::assertResult);
    }

    @Test
    public void testLoadJdbcSqlAliasParams() {
        testCall(db, "CALL apoc.load.jdbc($url,'testparams',['John'])",
                map("url", JDBC_DUCKDB),
                this::assertResult);
    }

    @Test
    public void testLoadJdbcError() {
        assertFails(db, "CALL apoc.load.jdbc(''jdbc:duckdb:testDB'','PERSON2')", map(), "Invalid input");
    }

    @Test
    public void testLoadJdbcProcessingError() {
        assertFails(db, "CALL apoc.load.jdbc(''jdbc:duckdb:testDB'','PERSON') YIELD row where row.name / 2 = 5 RETURN row", map(), "Invalid input");
    }

    @Test
    public void testLoadJdbcUpdate() {
        testCall(db, "CALL apoc.load.jdbcUpdate($url,'UPDATE PERSON SET SURNAME = ? WHERE NAME = ?', ['DOE', 'John'])",
                map("url", JDBC_DUCKDB),
                (row) -> assertEquals(Util.map("count", 1 ), row.get("row")));
    }

    @Test
    public void testLoadJdbcUpdateParams() {
        testCall(db, "CALL apoc.load.jdbcUpdate($url,'UPDATE PERSON SET SURNAME = ? WHERE NAME = ?',['John','John'])",
                map("url", JDBC_DUCKDB),
                (row) -> assertEquals(Util.map("count", 1 ), row.get("row")));
    }

    @Test
    public void testWithPeriodic() {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("delete from person");
            stmt.execute("select count(*) as size from person");
            stmt.getResultSet().next();
            int size = stmt.getResultSet().getInt("size");
            assertEquals(0 , size);
        } catch (Exception e) { }

        db.executeTransactionally("UNWIND range(1, 100) AS id CREATE (p:Person{id: id, name: 'Name ' + id, surname: 'Surname ' + id})");
        String query = "CALL apoc.periodic.iterate(\n" +
                "'MATCH (p:Person) RETURN p.name AS name, p.surname AS surname limit 1',\n" +
                "\"CALL apoc.load.jdbcUpdate($url, 'INSERT INTO PERSON(NAME, SURNAME) VALUES(?, ?)', [name, surname]) YIELD row RETURN 'DONE'\",\n" +
                "{batchSize: 20, iterateList: false, params: {url: $url}, parallel: true}\n" +
                ")\n" +
                "YIELD committedOperations, failedOperations, failedBatches, errorMessages\n" +
                "RETURN *";
        testCall(db,
                query,
                map("url", "jdbc:duckdb:testDB"),
                (row) -> {
                    try (Statement stmt = conn.createStatement()) {
                        stmt.execute("select count(*) as size from person");
                        stmt.getResultSet().next();
                        int size = stmt.getResultSet().getInt("size");
                        assertEquals(1, size);
                    } catch (Exception e) { }
                });
    }

    @Test
    public void testIterateJDBC() {
        final String jdbc = "CALL apoc.load.jdbc($url, 'PERSON',[]) YIELD row RETURN row";
        final String create = "CREATE (p:Person) SET p += row";
        testResult(db, "CALL apoc.periodic.iterate($jdbcQuery, $createQuery, {params: $params})",
                Util.map("params", Util.map("url", JDBC_DUCKDB), "jdbcQuery", jdbc, "createQuery", create), result -> {
                    Map<String, Object> row = Iterators.single(result);
                    assertEquals(1L, row.get("batches"));
                    assertEquals(1L, row.get("total"));
                });

        testCall(db,
                "MATCH (p:Person) return count(p) as count",
                row -> assertEquals(1L, row.get("count"))
        );
    }

    @Test
    public void testIssue3496() {
        String query = """
                SELECT
                    current_date() AS currDate,
                    get_current_timestamp() AS currTimestamp,
                    current_localtimestamp() AS currLocalTimestamp,
                    get_current_time() AS currTime,
                    CAST(get_current_timestamp() AS DATE) AS currDateFromTimestamp,
                    CAST(current_localtimestamp() AS DATE)  AS currDateFromLocalTimestamp;
                """;
        testCall(db, "CALL apoc.load.jdbc($url, $query)",
                Util.map("query", query, "url",JDBC_DUCKDB),
                r -> {
                    Map row = (Map) r.get("row");
                    assertEquals(6, row.size());

                    assertTrue(row.get("currDate") instanceof LocalDate);
                    assertTrue(row.get("currTime") instanceof LocalTime);
                    assertTrue(row.get("currTimestamp") instanceof ZonedDateTime);
                    assertTrue(row.get("currLocalTimestamp") instanceof LocalDateTime);
                    assertTrue(row.get("currDateFromTimestamp") instanceof LocalDate);
                    assertTrue(row.get("currDateFromLocalTimestamp") instanceof LocalDate);
                });
    }

    private void createPersonTableAndData(Connection conn) throws SQLException {
        try { conn.createStatement().execute("DROP TABLE PERSON"); } catch (SQLException se) {/*ignore*/}
        conn.createStatement().execute("CREATE TABLE PERSON (NAME varchar(50), SURNAME varchar(50), HIRE_DATE DATE, EFFECTIVE_FROM_DATE TIMESTAMP, TEST_TIME TIME, NULL_DATE DATE)");
        PreparedStatement ps = conn.prepareStatement("INSERT INTO PERSON values(?,null,?,?,?,?)");
        ps.setString(1, "John");
        ps.setDate(2, AbstractJdbcTest.hireDate);
        ps.setTimestamp(3, AbstractJdbcTest.effectiveFromDate);
        
        
        // workaround, DuckDB is shifted 1 hour later
        ps.setTime(4, AbstractJdbcTest.time);
        ps.setNull(5, Types.DATE);
        int rows = ps.executeUpdate();
        assertEquals(1, rows);
        ResultSet rs = conn.createStatement().executeQuery("SELECT NAME, HIRE_DATE, EFFECTIVE_FROM_DATE, TEST_TIME FROM PERSON");
        assertEquals(true, rs.next());
        assertEquals("John", rs.getString("NAME"));
        Assert.assertEquals(AbstractJdbcTest.hireDate.toLocalDate(), rs.getDate("HIRE_DATE").toLocalDate());
        Assert.assertEquals(AbstractJdbcTest.effectiveFromDate, rs.getTimestamp("EFFECTIVE_FROM_DATE"));

        // workaround, here the hour is 15:37
    //    Assert.assertEquals(AbstractJdbcTest.time, rs.getTime("TEST_TIME"));
        assertEquals(false, rs.next());
        rs.close();
    }

}
