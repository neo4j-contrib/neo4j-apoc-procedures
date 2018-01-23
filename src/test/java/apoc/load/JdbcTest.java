package apoc.load;

import apoc.ApocConfiguration;
import apoc.util.TestUtil;
import apoc.util.Util;
import org.junit.*;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.sql.*;
import java.time.Instant;
import java.util.Calendar;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testCall;
import static org.junit.Assert.assertEquals;

public class JdbcTest {

    private static GraphDatabaseService db;

    private static java.sql.Date hireDate = new java.sql.Date( new Calendar.Builder().setDate( 2017, 04, 25 ).build().getTimeInMillis() );

    private static java.sql.Timestamp effectiveFromDate = java.sql.Timestamp.from( Instant.now() );

    private static String urlOracle = "jdbc:oracle:thin:system/oracle@127.0.0.1:1521:XE";

    private static String urlPostgres = "jdbc:postgresql://localhost:5432/testApoc?user=postgres&password=password";

    @BeforeClass
    public static void setUp() throws Exception {
        db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        ApocConfiguration.initialize((GraphDatabaseAPI)db);
        ApocConfiguration.addToConfig(map("jdbc.derby.url","jdbc:derby:derbyDB"));
        TestUtil.registerProcedure(db,Jdbc.class);
        createPersonTableAndData();
        createApocTableOracle();
        createApocTablePostgres();
    }

    @AfterClass
    public static void tearDown() {
        db.shutdown();
    }

    @Test
    public void testLoadJdbc() throws Exception {
        testCall(db, "CALL apoc.load.jdbc('jdbc:derby:derbyDB','PERSON')",
                (row) -> assertEquals( Util.map("NAME", "John", "HIRE_DATE", hireDate.getTime(),"EFFECTIVE_FROM_DATE",
                        effectiveFromDate.getTime()), row.get("row")));
    }

    @Test
    public void testLoadJdbcSelect() throws Exception {
        testCall(db, "CALL apoc.load.jdbc('jdbc:derby:derbyDB','SELECT * FROM PERSON')",
                (row) -> assertEquals( Util.map("NAME", "John", "HIRE_DATE", hireDate.getTime(),"EFFECTIVE_FROM_DATE",
                        effectiveFromDate.getTime()), row.get("row")));
    }
    @Test
    public void testLoadJdbcSelectColumnNames() throws Exception {
        testCall(db, "CALL apoc.load.jdbc('jdbc:derby:derbyDB','SELECT NAME, HIRE_DATE AS DATE FROM PERSON')",
                (row) -> assertEquals( Util.map("NAME", "John", "DATE", hireDate.getTime()), row.get("row")));
    }

    @Test
    public void testLoadJdbcParams() throws Exception {
        testCall(db, "CALL apoc.load.jdbc('jdbc:derby:derbyDB','SELECT * FROM PERSON WHERE NAME = ?',['John'])", //  YIELD row RETURN row
                (row) -> assertEquals( Util.map("NAME", "John", "HIRE_DATE", hireDate.getTime(),"EFFECTIVE_FROM_DATE",
                        effectiveFromDate.getTime()), row.get("row")));
    }

    @Test
    public void testLoadJdbcKey() throws Exception {
        testCall(db, "CALL apoc.load.jdbc('derby','PERSON')",
                (row) -> assertEquals( Util.map("NAME", "John", "HIRE_DATE", hireDate.getTime(),"EFFECTIVE_FROM_DATE",
                        effectiveFromDate.getTime()), row.get("row")));
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
                (row) -> assertEquals(Util.map("count", 1), row.get("row")));
    }

    @Test
    public void testLoadJdbcUpdateParams() throws Exception {
        testCall(db, "CALL apoc.load.jdbcUpdate('jdbc:derby:derbyDB','UPDATE PERSON SET NAME = ? WHERE NAME = ?',['John','John'])",
                (row) -> assertEquals(Util.map("count", 1), row.get("row")));
    }

    @Test
    public void testLoadJdbcOracle() throws Exception {
        TestUtil.ignoreException(() -> {
            testCall(db, "CALL apoc.load.jdbc(\"" + urlOracle + "\", \"SELECT * FROM APOC\" )", (row) -> {
                        assertEquals("apoc", ((Map<String, Object>) row.get("row")).get("TESTBLOB"));
                        assertEquals("nclob", ((Map<String, Object>) row.get("row")).get("TESTNCLOB"));
                        assertEquals("test", ((Map<String, Object>) row.get("row")).get("TESTCLOB"));
                        assertEquals("test string", ((Map<String, Object>) row.get("row")).get("TESTSTRING"));
                        assertEquals(hireDate.getTime(), ((Map<String, Object>) row.get("row")).get("TESTDATE"));
                        assertEquals(0.5, ((Map<String, Object>) row.get("row")).get("TESTDOUBLE"));
                    });
        }, QueryExecutionException.class);
    }

    @Test
    public void testLoadJdbcPostgres() throws Exception {
        TestUtil.ignoreException(() -> {
            testCall(db, "CALL apoc.load.jdbc(\"" + urlPostgres + "\", \"SELECT * FROM person\" )", (row) -> {
                        assertEquals("John", ((Map<String, Object>) row.get("row")).get("testname"));
                        assertEquals("Green", ((Map<String, Object>) row.get("row")).get("testsurname"));
                        assertEquals(effectiveFromDate.getTime(), ((Map<String, Object>) row.get("row")).get("testtimestamp"));
                        assertEquals("a", ((Map<String, Object>) row.get("row")).get("testbytea"));
                        assertEquals(hireDate.getTime(), ((Map<String, Object>) row.get("row")).get("testdate"));
                        assertEquals("e4f92245-edb3-4cac-a2b4-3134c99e4209",((Map<String, Object>) row.get("row")).get("testuuid"));
                    });
        }, QueryExecutionException.class);
    }

    private static void createPersonTableAndData() throws ClassNotFoundException, SQLException {
        Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
        Connection conn = DriverManager.getConnection("jdbc:derby:derbyDB;create=true", new Properties());
        try { conn.createStatement().execute("DROP TABLE PERSON"); } catch (SQLException se) {/*ignore*/}
        conn.createStatement().execute("CREATE TABLE PERSON (NAME varchar(50), HIRE_DATE DATE, EFFECTIVE_FROM_DATE TIMESTAMP )");
        PreparedStatement ps = conn.prepareStatement("INSERT INTO PERSON values(?,?,?)");
        ps.setString(1, "John");
        ps.setDate(2, hireDate);
        ps.setTimestamp(3, effectiveFromDate);
        int rows = ps.executeUpdate();
        assertEquals(1, rows);
        ResultSet rs = conn.createStatement().executeQuery("SELECT NAME, HIRE_DATE, EFFECTIVE_FROM_DATE FROM PERSON");
        assertEquals(true, rs.next());
        assertEquals("John", rs.getString("NAME"));
        assertEquals(hireDate.getTime(), rs.getDate("HIRE_DATE").getTime());
        assertEquals(effectiveFromDate.getTime(), rs.getTimestamp("EFFECTIVE_FROM_DATE").getTime());
        assertEquals(false, rs.next());
        rs.close();
    }

    private static void createApocTableOracle() throws ClassNotFoundException, SQLException {
        Class.forName("oracle.jdbc.OracleDriver");
        TestUtil.ignoreException(() -> {
            try {
                Connection conn = DriverManager.getConnection(urlOracle);
                conn.createStatement().execute("DROP TABLE apoc");
                conn.createStatement().execute("CREATE TABLE apoc ( testTime timestamp(0), testBlob blob, testNclob nclob, "
                        + "testDate date, testDouble binary_double, testClob clob, testString varchar2(100))");
                PreparedStatement ps = conn.prepareStatement("INSERT INTO apoc values(?,?,?,?,?,?,?)");
                ps.setTimestamp(1, effectiveFromDate);
                InputStream is = new ByteArrayInputStream("apoc".getBytes());
                ps.setBlob(2, is);
                Reader reader = new StringReader("nclob content");
                ps.setNClob(3, reader);
                ps.setDate(4, hireDate);
                ps.setDouble(5, 0.5);
                Clob clob = conn.createClob();
                clob.setString(1, "test clob");
                ps.setClob(6, clob);
                ps.setString(7, "test string");
                ps.execute();
            } catch (SQLException se) {
            }
        }, SQLException.class);
    }

    private static void createApocTablePostgres() throws ClassNotFoundException, SQLException {
        Class.forName("org.postgresql.Driver");
        TestUtil.ignoreException(() -> {
            try {
                Connection conn = DriverManager.getConnection (urlPostgres);
                conn.createStatement().execute("DROP TABLE IF EXISTS person");
                conn.createStatement().execute("CREATE TABLE person ( testid int, testname character varying, testsurname character varying, " +
                        "testtimestamp timestamp, testbytea bytea, testdate date, testuuid uuid)");
                PreparedStatement ps = conn.prepareStatement("INSERT INTO person values(?,?,?,?,?,?,?)");
                ps.setInt(1,new Integer(2));
                ps.setString(2,"John");
                ps.setString(3,"Green");
                ps.setTimestamp(4, effectiveFromDate);
                ps.setBytes(5, "a".getBytes());
                ps.setDate(6, hireDate);
                ps.setObject(7, UUID.fromString("e4f92245-edb3-4cac-a2b4-3134c99e4209"));
                ps.execute();
            } catch (SQLException se) {
            }
        }, SQLException.class);
    }
}
