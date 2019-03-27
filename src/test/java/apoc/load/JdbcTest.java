package apoc.load;

import apoc.ApocConfiguration;
import apoc.util.TestUtil;
import apoc.util.Util;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.junit.*;
import org.junit.rules.TestName;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.sql.*;
import java.time.*;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testResult;
import static java.util.Collections.emptyList;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

public class JdbcTest extends AbstractJdbcTest {

    private GraphDatabaseService db;

    private Connection conn;

    @Rule
    public TestName testName = new TestName();

    private static final String TEST_WITH_AUTHENTICATION = "WithAuthentication";

    @Before
    public void setUp() throws Exception {
        db = TestUtil.apocGraphDatabaseBuilder().newGraphDatabase();
        ApocConfiguration.initialize((GraphDatabaseAPI)db);
        ApocConfiguration.addToConfig(map("jdbc.derby.url","jdbc:derby:derbyDB"));
        ApocConfiguration.addToConfig(map("jdbc.test.sql","SELECT * FROM PERSON"));
        ApocConfiguration.addToConfig(map("jdbc.testparams.sql","SELECT * FROM PERSON WHERE NAME = ?"));
        TestUtil.registerProcedure(db,Jdbc.class);
        createPersonTableAndData();
    }

    @After
    public void tearDown() throws SQLException {
        db.shutdown();
        conn.close();
        try {
            if (testName.getMethodName().endsWith(TEST_WITH_AUTHENTICATION)) {
                DriverManager.getConnection("jdbc:derby:derbyDB;user=apoc;password=Ap0c!#Db;shutdown=true");
            } else {
                DriverManager.getConnection("jdbc:derby:derbyDB;shutdown=true");
            }
        } catch (SQLException e) {
            // DerbyDB shutdown always raise a SQLException, see: http://db.apache.org/derby/docs/10.14/devguide/tdevdvlp20349.html
            if (((e.getErrorCode() == 45000)
                    && ("08006".equals(e.getSQLState())))) {
                // Note that for single database shutdown, the expected
                // SQL state is "08006", and the error code is 45000.
            } else {
                throw e;
            }
        }
        System.clearProperty("derby.connection.requireAuthentication");
        System.clearProperty("derby.user.apoc");
    }

    @Test
    public void testLoadJdbc() throws Exception {
        testCall(db, "CALL apoc.load.jdbc('jdbc:derby:derbyDB','PERSON')",
                (row) -> assertResult(row));
    }

    @Test
    public void testLoadJdbcSelect() throws Exception {
        testCall(db, "CALL apoc.load.jdbc('jdbc:derby:derbyDB','SELECT * FROM PERSON')",
                (row) -> assertResult(row));
    }
    @Test
    public void testLoadJdbcSelectColumnNames() throws Exception {
        testCall(db, "CALL apoc.load.jdbc('jdbc:derby:derbyDB','SELECT NAME, HIRE_DATE AS DATE FROM PERSON')",
                (row) -> assertEquals(Util.map("NAME", "John", "DATE", hireDate.toLocalDate()), row.get("row")));
    }

    @Test
    public void testLoadJdbcParams() throws Exception {
        testCall(db, "CALL apoc.load.jdbc('jdbc:derby:derbyDB','SELECT * FROM PERSON WHERE NAME = ?',['John'])", //  YIELD row RETURN row
                (row) -> assertResult(row));
    }

    @Test
    public void testLoadJdbcParamsWithConfigLocalDateTime() throws Exception {
        testCall(db, "CALL apoc.load.jdbc('jdbc:derby:derbyDB','SELECT * FROM PERSON WHERE NAME = ?',['John'])",
                (row) -> assertResult(row));

        ZoneId asiaTokio = ZoneId.of("Asia/Tokyo");

        testCall(db, "CALL apoc.load.jdbc('jdbc:derby:derbyDB','SELECT * FROM PERSON WHERE NAME = ?',['John'], {config})",
                map("config", map("timezone", asiaTokio.toString())),
                (row) -> assertEquals(Util.map("NAME", "John", "SURNAME", null,
                            "HIRE_DATE", hireDate.toLocalDate(),
                            "EFFECTIVE_FROM_DATE", effectiveFromDate.toInstant().atZone(asiaTokio).toOffsetDateTime(),
                            "TEST_TIME", time.toLocalTime(),
                            "NULL_DATE", null), row.get("row")
                )
        );
    }

    @Test(expected = RuntimeException.class)
    public void testLoadJdbcParamsWithWrongTimezoneValue() throws Exception {
        db.execute("CALL apoc.load.jdbc('jdbc:derby:derbyDB','SELECT * FROM PERSON WHERE NAME = ?',['John'], {timezone: {timezone}})",
                map("timezone", "Italy/Pescara")).next();
    }

    @Test
    public void testLoadJdbcKey() throws Exception {
        testCall(db, "CALL apoc.load.jdbc('derby','PERSON')",
                (row) -> assertResult(row));
    }

    @Test
    public void testLoadJdbcSqlAlias() throws Exception {
        testCall(db, "CALL apoc.load.jdbc('derby','test')",
                (row) -> assertResult(row));
    }

    @Test
    public void testLoadJdbcSqlAliasParams() throws Exception {
        testCall(db, "CALL apoc.load.jdbc('jdbc:derby:derbyDB','testparams',['John'])", //  YIELD row RETURN row
                (row) -> assertResult(row));
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
        testCall(db, "CALL apoc.load.jdbcUpdate('jdbc:derby:derbyDB','UPDATE PERSON SET SURNAME = ? WHERE NAME = ?', ['DOE', 'John'])",
                (row) -> assertEquals(Util.map("count", 1 ), row.get("row")));
    }

    @Test
    public void testLoadJdbcUpdateParams() throws Exception {
        testCall(db, "CALL apoc.load.jdbcUpdate('jdbc:derby:derbyDB','UPDATE PERSON SET SURNAME = ? WHERE NAME = ?',['John','John'])",
                (row) -> assertEquals(Util.map("count", 1 ), row.get("row")));
    }

    @Test
    public void testLoadJdbcWithSpecialCharWithAuthentication() {
        db.execute("CALL apoc.load.jdbc({url}, 'PERSON',[],{credentials:{user:'apoc',password:'Ap0c!#Db'}})", Util.map("url","jdbc:derby:derbyDB")).next();
    }

    @Test
    public void testLoadJdbcUpdateParamsUrlWithSpecialCharWithAuthentication() throws Exception {
        testCall(db, "CALL apoc.load.jdbcUpdate('jdbc:derby:derbyDB','UPDATE PERSON SET NAME = ? WHERE NAME = ?',['John','John'],{credentials:{user:'apoc',password:'Ap0c!#Db'}})",
                (row) -> assertEquals(Util.map("count", 1 ), row.get("row")));
    }

    @Test(expected = QueryExecutionException.class)
    public void testLoadJdbcUrlWithSpecialCharWithEmptyUserWithAuthentication() throws Exception {
        try {
            db.execute("CALL apoc.load.jdbc({url}, 'PERSON',[],{credentials:{user:'',password:'Ap0c!#Db'}})", Util.map("url","jdbc:derby:derbyDB")).next();
        } catch (IllegalArgumentException e) {
            Throwable except = ExceptionUtils.getRootCause(e);
            assertTrue(except instanceof IllegalArgumentException);
            assertEquals("In config param credentials must be passed both user and password.", except.getMessage());
            throw e;
        }

    }

    @Test(expected = QueryExecutionException.class)
    public void testLoadJdbcUrlWithSpecialCharWithoutUserWithAuthentication() throws Exception {
        try {
            db.execute("CALL apoc.load.jdbc({url}, 'PERSON',[],{credentials:{password:'Ap0c!#Db'}})", Util.map("url","jdbc:derby:derbyDB")).next();
        } catch (IllegalArgumentException e) {
            Throwable except = ExceptionUtils.getRootCause(e);
            assertTrue(except instanceof IllegalArgumentException);
            assertEquals("In config param credentials must be passed both user and password.", except.getMessage());
            throw e;
        }

    }

    @Test(expected = QueryExecutionException.class)
    public void testLoadJdbcUrlWithSpecialCharWithEmptyPasswordWithAuthentication() throws Exception {
        try {
            db.execute("CALL apoc.load.jdbc({url}, 'PERSON',[],{credentials:{user:'apoc',password:''}})", Util.map("url","jdbc:derby:derbyDB")).next();
        } catch (IllegalArgumentException e) {
            Throwable except = ExceptionUtils.getRootCause(e);
            assertTrue(except instanceof IllegalArgumentException);
            assertEquals("In config param credentials must be passed both user and password.", except.getMessage());
            throw e;
        }

    }

    @Test(expected = QueryExecutionException.class)
    public void testLoadJdbcUrlWithSpecialCharWithoutPasswordWithAuthentication() throws Exception {
        try {
            db.execute("CALL apoc.load.jdbc({url}, 'PERSON',[],{credentials:{user:'apoc'}})", Util.map("url","jdbc:derby:derbyDB")).next();
        } catch (IllegalArgumentException e) {
            Throwable except = ExceptionUtils.getRootCause(e);
            assertTrue(except instanceof IllegalArgumentException);
            assertEquals("In config param credentials must be passed both user and password.", except.getMessage());
            throw e;
        }

    }

    private void createPersonTableAndData() throws ClassNotFoundException, SQLException, IllegalAccessException, InstantiationException {
        Class.forName("org.apache.derby.jdbc.EmbeddedDriver").newInstance(); // The JDBC specification does not recommend calling newInstance(), but adding a newInstance() call guarantees that Derby will be booted on any JVM. See: http://db.apache.org/derby/docs/10.14/devguide/tdevdvlp20349.html
        if (testName.getMethodName().endsWith(TEST_WITH_AUTHENTICATION)) {
            System.setProperty("derby.connection.requireAuthentication", "true");
            System.setProperty("derby.user.apoc", "Ap0c!#Db");
            conn = DriverManager.getConnection("jdbc:derby:derbyDB;user=apoc;password=Ap0c!#Db;create=true");
        } else {
            conn = DriverManager.getConnection("jdbc:derby:derbyDB;create=true");
        }
        try { conn.createStatement().execute("DROP TABLE PERSON"); } catch (SQLException se) {/*ignore*/}
        conn.createStatement().execute("CREATE TABLE PERSON (NAME varchar(50), SURNAME varchar(50), HIRE_DATE DATE, EFFECTIVE_FROM_DATE TIMESTAMP, TEST_TIME TIME, NULL_DATE DATE)");
        PreparedStatement ps = conn.prepareStatement("INSERT INTO PERSON values(?,null,?,?,?,?)");
        ps.setString(1, "John");
        ps.setDate(2, hireDate);
        ps.setTimestamp(3, effectiveFromDate);
        ps.setTime(4, time);
        ps.setNull(5, Types.DATE);
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