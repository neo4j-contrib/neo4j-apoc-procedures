package apoc.load;

import apoc.ApocConfiguration;
import apoc.util.TestUtil;
import apoc.util.Util;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.junit.*;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.sql.*;
import java.time.*;
import java.util.Calendar;
import java.util.Properties;

import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testResult;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

public class JdbcTest {

    private static GraphDatabaseService db;

    private static java.sql.Date hireDate = new java.sql.Date( new Calendar.Builder().setDate(2017, 04, 25).build().getTimeInMillis() );

    private static java.sql.Timestamp effectiveFromDate = java.sql.Timestamp.from(Instant.now());

    private static java.sql.Time time = java.sql.Time.valueOf("15:37:00");

    @BeforeClass
    public static void setUp() throws Exception {
        db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        ApocConfiguration.initialize((GraphDatabaseAPI)db);
        ApocConfiguration.addToConfig(map("jdbc.derby.url","jdbc:derby:derbyDB"));
        TestUtil.registerProcedure(db,Jdbc.class);
        createPersonTableAndData();
    }

    @AfterClass
    public static void tearDown() {
        db.shutdown();
    }

    @Test
    public void testLoadJdbc() throws Exception {
        testCall(db, "CALL apoc.load.jdbc('jdbc:derby:derbyDB','PERSON')",
                (row) -> assertEquals( Util.map("NAME", "John", "HIRE_DATE", hireDate.toLocalDate(), "EFFECTIVE_FROM_DATE",
                        effectiveFromDate.toLocalDateTime(), "TEST_TIME", time.toLocalTime()), row.get("row")));
    }

    @Test
    public void testLoadJdbcSelect() throws Exception {
        testCall(db, "CALL apoc.load.jdbc('jdbc:derby:derbyDB','SELECT * FROM PERSON')",
                (row) -> assertEquals( Util.map("NAME", "John", "HIRE_DATE", hireDate.toLocalDate(),"EFFECTIVE_FROM_DATE",
                        effectiveFromDate.toLocalDateTime(), "TEST_TIME", time.toLocalTime()), row.get("row")));
    }
    @Test
    public void testLoadJdbcSelectColumnNames() throws Exception {
        testCall(db, "CALL apoc.load.jdbc('jdbc:derby:derbyDB','SELECT NAME, HIRE_DATE AS DATE FROM PERSON')",
                (row) -> assertEquals( Util.map("NAME", "John", "DATE", hireDate.toLocalDate()), row.get("row")));
    }

    @Test
    public void testLoadJdbcParams() throws Exception {
        testCall(db, "CALL apoc.load.jdbc('jdbc:derby:derbyDB','SELECT * FROM PERSON WHERE NAME = ?',['John'])", //  YIELD row RETURN row
                (row) -> assertEquals( Util.map("NAME", "John",
                        "HIRE_DATE", hireDate.toLocalDate(),
                        "EFFECTIVE_FROM_DATE", effectiveFromDate.toLocalDateTime(),
                        "TEST_TIME", time.toLocalTime()), row.get("row")));
    }

    @Test
    public void testLoadJdbcParamsWithConfigLocalDateTime() throws Exception {
        testCall(db, "CALL apoc.load.jdbc('jdbc:derby:derbyDB','SELECT * FROM PERSON WHERE NAME = ?',['John'])",
                (row) -> assertEquals( Util.map("NAME", "John", "HIRE_DATE", hireDate.toLocalDate(), "EFFECTIVE_FROM_DATE",
                        effectiveFromDate.toLocalDateTime(), "TEST_TIME", time.toLocalTime()), row.get("row")));

        ZoneId asiaTokio = ZoneId.of("Asia/Tokyo");
        testCall(db, "CALL apoc.load.jdbc('jdbc:derby:derbyDB','SELECT * FROM PERSON WHERE NAME = ?',['John'], {config})",
                map("config", map("timezone", asiaTokio.toString())),
                (row) -> {
                    assertEquals( Util.map("NAME", "John",
                            "HIRE_DATE", hireDate.toLocalDate(),
                            "EFFECTIVE_FROM_DATE", effectiveFromDate.toInstant().atZone(asiaTokio).toOffsetDateTime(),
                            "TEST_TIME", time.toLocalTime()), row.get("row"));

                });
    }

    @Test(expected = RuntimeException.class)
    public void testLoadJdbcParamsWithWrongTimezoneValue() throws Exception {
        db.execute("CALL apoc.load.jdbc('jdbc:derby:derbyDB','SELECT * FROM PERSON WHERE NAME = ?',['John'], {timezone: {timezone}})",
                map("timezone", "Italy/Pescara")).next();
    }

    @Test
    public void testLoadJdbcKey() throws Exception {
        testCall(db, "CALL apoc.load.jdbc('derby','PERSON')",
                (row) -> assertEquals( Util.map("NAME", "John",
                        "HIRE_DATE", hireDate.toLocalDate(),
                        "EFFECTIVE_FROM_DATE", effectiveFromDate.toLocalDateTime(),
                        "TEST_TIME", time.toLocalTime()), row.get("row")));

    }

    @Test(expected = RuntimeException.class)
    public void testLoadJdbcError() throws Exception {
        db.execute("CALL apoc.load.jdbc(''jdbc:derby:derbyDB'','PERSON2')").next();
        // todo count derby connections?
    }
    @Test(expected = RuntimeException.class)
    public void testLoadJdbcProcessingError() throws Exception {
        db.execute("CALL apoc.load.jdbc(''jdbc:derby:derbyDB'','PERSON') YIELD row where row.name / 2 = 5 RETURN row").next();
        // todo count derby connections?
    }

    @Test
    public void testLoadJdbcUpdate() throws Exception {
        testCall(db, "CALL apoc.load.jdbcUpdate('jdbc:derby:derbyDB','UPDATE PERSON SET NAME = \\\'John\\\' WHERE NAME = \\\'John\\\'')",
                (row) -> assertEquals( Util.map("count", 1 ), row.get("row")));
    }

    @Test
    public void testLoadJdbcUpdateParams() throws Exception {
        testCall(db, "CALL apoc.load.jdbcUpdate('jdbc:derby:derbyDB','UPDATE PERSON SET NAME = ? WHERE NAME = ?',['John','John'])",
                (row) -> assertEquals( Util.map("count", 1 ), row.get("row")));
    }

    private static void createPersonTableAndData() throws ClassNotFoundException, SQLException {
        Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
        Connection conn = DriverManager.getConnection("jdbc:derby:derbyDB;create=true", new Properties());
        try { conn.createStatement().execute("DROP TABLE PERSON"); } catch (SQLException se) {/*ignore*/}
        conn.createStatement().execute("CREATE TABLE PERSON (NAME varchar(50), HIRE_DATE DATE, EFFECTIVE_FROM_DATE TIMESTAMP, TEST_TIME TIME)");
        PreparedStatement ps = conn.prepareStatement("INSERT INTO PERSON values(?,?,?,?)");
        ps.setString(1, "John");
        ps.setDate(2, hireDate);
        ps.setTimestamp(3, effectiveFromDate);
        ps.setTime(4, time);
        int rows = ps.executeUpdate();
        assertEquals(1, rows);
        ResultSet rs = conn.createStatement().executeQuery("SELECT NAME, HIRE_DATE, EFFECTIVE_FROM_DATE, TEST_TIME FROM PERSON");
        assertEquals(true, rs.next());
        assertEquals("John", rs.getString("NAME"));
        assertEquals(hireDate.toLocalDate(), rs.getDate("HIRE_DATE").toLocalDate());
        assertEquals(effectiveFromDate, rs.getTimestamp("EFFECTIVE_FROM_DATE"));
        assertEquals(time, rs.getTime("TEST_TIME"));
        assertEquals(false, rs.next());
        rs.close();
    }

    @Test(expected = QueryExecutionException.class)
    public void testLoadJdbcWrongKey() throws Exception {
        try {
            testResult(db, "CALL apoc.load.jdbc('derbyy','PERSON')", (r) -> {});
        } catch (QueryExecutionException e) {
            Throwable except = ExceptionUtils.getRootCause(e);
            assertTrue(except instanceof RuntimeException);
            assertEquals("No apoc.jdbc.derbyy.url url specified", except.getMessage());
            throw e;
        }

    }

}