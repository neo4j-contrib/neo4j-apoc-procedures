package apoc.load;

import apoc.ApocConfiguration;
import apoc.util.TestUtil;
import apoc.util.Util;
import org.junit.*;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.sql.*;
import java.time.Instant;
import java.util.Calendar;
import java.util.Properties;

import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testCall;
import static org.junit.Assert.assertEquals;

public class JdbcTest {

    private static GraphDatabaseService db;

    private static java.sql.Date hireDate = new java.sql.Date( new Calendar.Builder().setDate( 2017, 04, 25 ).build().getTimeInMillis() );

    private static java.sql.Timestamp effectiveFromDate = java.sql.Timestamp.from( Instant.now() );

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
}
