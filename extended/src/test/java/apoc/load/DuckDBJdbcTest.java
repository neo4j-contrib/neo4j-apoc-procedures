package apoc.load;

import apoc.periodic.Periodic;
import apoc.util.MapUtil;
import apoc.util.TestUtil;
import apoc.util.Util;
import apoc.util.collection.Iterators;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.jupiter.api.AfterAll;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Types;
import java.time.ZoneId;
import java.util.Calendar;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;

import static apoc.ApocConfig.apocConfig;
import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testResult;
import static org.junit.Assert.assertEquals;

public class DuckDBJdbcTest extends AbstractJdbcTest {

    /*
    TODO : scrivere sulla PR
    When using the jdbc:duckdb: URL alone, an in-memory database is created. 
    Note that for an in-memory database no data is persisted to disk (i.e., all data is lost when you exit the Java program). 
    If you would like to access or create a persistent database, append its file name after the path. For example, if your database is stored in /tmp/my_database, use the JDBC URL jdbc:duckdb:/tmp/my_database to create a connection to it.
     */
    
    
    /*
    PreparedStatement ps = conn.prepareStatement("CREATE TEMP TABLE movies AS SELECT * FROM rs");
    ps.setObject(1, map("a", 1, "b", 2));
    ps.executeQuery();
     */
    
    public String JDBC_DUCKDB = null;
    
    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();

    private Connection conn;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Before
    public void setUp() throws Exception {
        // TODO - scrivere sulla PR: se mettiamo lo stesso JDBC url per tutti i test, flakily un test a caso rimane in pending senza apparente motivo 
        //  tried also with TemporaryFolder storeDir but it fails with error: `IO Error: Could not read from file "/var/folders/kn/v9jyxl9s67z5qzf_mc8d62sm0000gp/T/junit6152729252456113118/junit894048343433219285": Is a directory`
        //  despite here https://duckdb.org/docs/api/java#installation, defining a folder to create an embedded seems to be possible
        JDBC_DUCKDB = "jdbc:duckdb:" + temporaryFolder.newFolder() + "/testDB";// UUID.randomUUID();
        apocConfig().setProperty("apoc.jdbc.duckdb.url", JDBC_DUCKDB);
        apocConfig().setProperty("apoc.jdbc.test.sql","SELECT * FROM PERSON");
        apocConfig().setProperty("apoc.jdbc.testparams.sql","SELECT * FROM PERSON WHERE NAME = ?");
        TestUtil.registerProcedure(db, Jdbc.class, Periodic.class);
        
        conn = DriverManager.getConnection(JDBC_DUCKDB);
        createPersonTableAndData();
    }

    @After
    public void tearDown() throws SQLException {
        conn.close();
//        try {
//            DriverManager.getConnection(JDBC_DUCKDB);
//        } catch (SQLException e) {
//            // DerbyDB shutdown always raise a SQLException, see: http://db.apache.org/derby/docs/10.14/devguide/tdevdvlp20349.html
//            if (((e.getErrorCode() == 45000)
//                    && ("08006".equals(e.getSQLState())))) {
//                // Note that for single database shutdown, the expected
//                // SQL state is "08006", and the error code is 45000.
//            } else {
//                throw e;
//            }
//        }
//        System.clearProperty("derby.connection.requireAuthentication");
//        System.clearProperty("derby.user.apoc");
    }

    /*
    TODO
    conn.createStatement().execute("""
        CREATE TEMPORARY TABLE movies_data AS 
SELECT * FROM 
(VALUES
    ('Keanu Reeves', 'Sci-Fi', 3),
    ('Carrie-Anne Moss', 'Sci-Fi', 2),
    ('Laurence Fishburne', 'Sci-Fi', 3),
    ('Keanu Reeves', 'Action', 4),
    ('Will Smith', 'Action', 5)
) AS t(actor, genre, movies_count);

        """);
//ps.setObject(1, map("a", 1, "b", 2));
ps.executeQuery();
//conn.createStatement().executeQuery("CREATE TEMPORARY TABLE movies AS SELECT * FROM ?", map())
     */

    // todo - this is the test of issue 3610
    @Test
    public void testLoadJdbcAnalytics() {
        // -- create temporary table
        System.out.println("DuckDBJdbcTest.testLoadJdbcAnalytics");
        
        // -- query with temporary table
        System.out.println("DuckDBJdbcTest.testLoadJdbcAnalytics");
    }
    
    // TODO:
        // 1: create temp table
        // 2: rank() function by default, otherwise other, configurable?
            // fare leva sulle apoc.db che creano dei csv???
            // forse no, però potrei fare export e poi load csv???
        // 3: config: default DUCKDB

    /*
    Temporary Tables
    Temporary tables can be created using the CREATE TEMP TABLE or the CREATE TEMPORARY TABLE statement (see diagram below). Temporary tables are session scoped (similar to PostgreSQL for example), meaning that only the specific connection that created them can access them, and once the connection to DuckDB is closed they will be automatically dropped. Temporary tables reside in memory rather than on disk (even when connecting to a persistent DuckDB), but if the temp_directory configuration is set when connecting or with a SET command, data will be spilled to disk if memory becomes constrained.
    
    Create a temporary table from a CSV file (automatically detecting column names and types):
    
    CREATE TEMP TABLE t1 AS
        SELECT *
        FROM read_csv('path/file.csv');
    
    Allow temporary tables to off-load excess memory to disk:
    
    SET temp_directory = '/path/to/directory/';
    
    Temporary tables are part of the temp.main schema. While discouraged, their names can overlap with the names of the regular database tables. In these cases, use their fully qualified name, e.g., temp.main.t1, for disambiguation.
     */


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

    /*
    conn.createStatement().ex("""
        CREATE TEMPORARY TABLE movies_data AS 
        SELECT * FROM 
        (VALUES
            ('Keanu Reeves', 'Sci-Fi', 3),
            ('Carrie-Anne Moss', 'Sci-Fi', 2),
            ('Laurence Fishburne', 'Sci-Fi', 3),
            ('Keanu Reeves', 'Action', 4),
            ('Will Smith', 'Action', 5)
        ) AS t(actor, genre, movies_count);

        """);
     */
    
    
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
                            "EFFECTIVE_FROM_DATE", AbstractJdbcTest.effectiveFromDate.toInstant().atZone(asiaTokio).toOffsetDateTime().toZonedDateTime(), // todo investigate why by only changing the procedure mode returned class type changes
                            "TEST_TIME", AbstractJdbcTest.time.toLocalTime(),
                            "NULL_DATE", null);
                    Map<String, Object> rowColumn = (Map<String, Object>) row.get("row");

                    expected.keySet().forEach( k -> {
                        assertEquals(expected.get(k), rowColumn.get(k));
                    });
                    assertEquals(expected, rowColumn);
                }

        );
    }

    @Test
    public void testLoadJdbcParamsWithWrongTimezoneValue() {
        thrown.expect(QueryExecutionException.class);
        thrown.expectMessage("Failed to invoke procedure `apoc.load.jdbc`: Caused by: java.lang.IllegalArgumentException: The timezone field contains an error: Unknown time-zone ID: Italy/Pescara");
        TestUtil.singleResultFirstColumn(db,"CALL apoc.load.jdbc('jdbc:duckdb:testDB','SELECT * FROM PERSON WHERE NAME = ?',['John'], {timezone: $timezone})",
                map("timezone", "Italy/Pescara"));
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
        testCall(db, "CALL apoc.load.jdbc($url,'testparams',['John'])", //  YIELD row RETURN row
                map("url", JDBC_DUCKDB),
                this::assertResult);
    }

    @Test
    public void testLoadJdbcError() {
        thrown.expect(QueryExecutionException.class);
        thrown.expectMessage("Invalid input");
        db.executeTransactionally("CALL apoc.load.jdbc(''jdbc:duckdb:testDB'','PERSON2')");
    }

    @Test
    public void testLoadJdbcProcessingError() {
        thrown.expect(QueryExecutionException.class);
        thrown.expectMessage("Invalid input");
        db.executeTransactionally("CALL apoc.load.jdbc(''jdbc:duckdb:testDB'','PERSON') YIELD row where row.name / 2 = 5 RETURN row");
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

    private void createPersonTableAndData() throws SQLException {
        try { conn.createStatement().execute("DROP TABLE PERSON"); } catch (SQLException se) {/*ignore*/}
        conn.createStatement().execute("CREATE TABLE PERSON (NAME varchar(50), SURNAME varchar(50), HIRE_DATE DATE, EFFECTIVE_FROM_DATE TIMESTAMP, TEST_TIME TIME, NULL_DATE DATE)");
        PreparedStatement ps = conn.prepareStatement("INSERT INTO PERSON values(?,null,?,?,?,?)");
        ps.setString(1, "John");
        ps.setDate(2, AbstractJdbcTest.hireDate);
        ps.setTimestamp(3, AbstractJdbcTest.effectiveFromDate);
        
        
        // TODO workaround, DuckDB is shifted 1 hour later
        //      VEDERE SE c'è un modo più carino, invece di passare un valore manuale 1 ora indietro a AbstractJdbcTest.time
        //      altrimenti va bene così
        
        // since currenty neither         ps.setTime(4, AbstractJdbcTest.time, Calendar.getInstance(TimeZone.getTimeZone("UTC")));
        // or  ps.setObject(1, localTime); is possible
        ps.setTime(4, java.sql.Time.valueOf("16:37:00"));
//        ps.setObject(4, AbstractJdbcTest.time.toLocalTime());//, Calendar.getInstance(TimeZone.getTimeZone("UTC")));
        ps.setNull(5, Types.DATE);
        int rows = ps.executeUpdate();
        assertEquals(1, rows);
        ResultSet rs = conn.createStatement().executeQuery("SELECT NAME, HIRE_DATE, EFFECTIVE_FROM_DATE, TEST_TIME FROM PERSON");
        assertEquals(true, rs.next());
        assertEquals("John", rs.getString("NAME"));
        Assert.assertEquals(AbstractJdbcTest.hireDate.toLocalDate(), rs.getDate("HIRE_DATE").toLocalDate());
        Assert.assertEquals(AbstractJdbcTest.effectiveFromDate, rs.getTimestamp("EFFECTIVE_FROM_DATE"));

        // workaround, here the hour is 15:37
        Assert.assertEquals(AbstractJdbcTest.time, rs.getTime("TEST_TIME"));
        assertEquals(false, rs.next());
        rs.close();
    }

}
