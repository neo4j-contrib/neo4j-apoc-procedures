package apoc.load;

import apoc.periodic.Periodic;
import apoc.util.TestUtil;
import apoc.util.Util;
import org.junit.*;
import org.junit.rules.ExpectedException;
import org.junit.rules.TestName;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.internal.helpers.collection.MapUtil;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.time.ZoneId;
import java.util.Map;

import static apoc.ApocConfig.apocConfig;
import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testResult;
import static org.junit.Assert.assertEquals;

public class JdbcTest extends AbstractJdbcTest {

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();

    private Connection conn;

    @Rule
    public TestName testName = new TestName();

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private static final String TEST_WITH_AUTHENTICATION = "WithAuthentication";

    @Before
    public void setUp() throws Exception {
        apocConfig().setProperty("apoc.jdbc.derby.url","jdbc:derby:derbyDB");
        apocConfig().setProperty("apoc.jdbc.test.sql","SELECT * FROM PERSON");
        apocConfig().setProperty("apoc.jdbc.testparams.sql","SELECT * FROM PERSON WHERE NAME = ?");
        TestUtil.registerProcedure(db,Jdbc.class, Periodic.class);
        createPersonTableAndData();
    }

    @After
    public void tearDown() throws SQLException {
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
    public void testLoadJdbcWithFetchSize() throws Exception {
        testCall(db, "CALL apoc.load.jdbc('jdbc:derby:derbyDB','PERSON', null, {fetchSize: 100})",
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
                (row) -> assertEquals(Util.map("NAME", "John", "DATE", AbstractJdbcTest.hireDate.toLocalDate()), row.get("row")));
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

        testCall(db, "CALL apoc.load.jdbc('jdbc:derby:derbyDB','SELECT * FROM PERSON WHERE NAME = ?',['John'], $config)",
                map("config", map("timezone", asiaTokio.toString())),
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
    public void testLoadJdbcParamsWithWrongTimezoneValue() throws Exception {
        thrown.expect(QueryExecutionException.class);
        thrown.expectMessage("Failed to invoke procedure `apoc.load.jdbc`: Caused by: java.lang.IllegalArgumentException: The timezone field contains an error: Unknown time-zone ID: Italy/Pescara");
        TestUtil.singleResultFirstColumn(db,"CALL apoc.load.jdbc('jdbc:derby:derbyDB','SELECT * FROM PERSON WHERE NAME = ?',['John'], {timezone: $timezone})",
                map("timezone", "Italy/Pescara"));
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

    @Test
    public void testLoadJdbcError() throws Exception {
        thrown.expect(QueryExecutionException.class);
        thrown.expectMessage("Invalid input");
        db.executeTransactionally("CALL apoc.load.jdbc(''jdbc:derby:derbyDB'','PERSON2')");
        // TODO: count derby connections?
    }

    @Test
    public void testLoadJdbcProcessingError() throws Exception {
        thrown.expect(QueryExecutionException.class);
        thrown.expectMessage("Invalid input");
        db.executeTransactionally("CALL apoc.load.jdbc(''jdbc:derby:derbyDB'','PERSON') YIELD row where row.name / 2 = 5 RETURN row");
        // TODO: count derby connections?
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
        db.executeTransactionally("CALL apoc.load.jdbc($url, 'PERSON',[],{credentials:{user:'apoc',password:'Ap0c!#Db'}})", Util.map("url","jdbc:derby:derbyDB"));
    }

    @Test
    public void testLoadJdbcUpdateParamsUrlWithSpecialCharWithAuthentication() throws Exception {
        testCall(db, "CALL apoc.load.jdbcUpdate('jdbc:derby:derbyDB','UPDATE PERSON SET NAME = ? WHERE NAME = ?',['John','John'],{credentials:{user:'apoc',password:'Ap0c!#Db'}})",
                (row) -> assertEquals(Util.map("count", 1 ), row.get("row")));
    }

    @Test
    public void testLoadJdbcUrlWithSpecialCharWithEmptyUserWithAuthentication() throws Exception {
        thrown.expect(QueryExecutionException.class);
        thrown.expectMessage("In config param credentials must be passed both user and password.");
        TestUtil.singleResultFirstColumn(db,"CALL apoc.load.jdbc($url, 'PERSON',[],{credentials:{user:'',password:'Ap0c!#Db'}})", Util.map("url","jdbc:derby:derbyDB"));
    }

    @Test
    public void testLoadJdbcUrlWithSpecialCharWithoutUserWithAuthentication() throws Exception {
        thrown.expect(QueryExecutionException.class);
        thrown.expectMessage("In config param credentials must be passed both user and password.");
        TestUtil.singleResultFirstColumn(db, "CALL apoc.load.jdbc($url, 'PERSON',[],{credentials:{password:'Ap0c!#Db'}})", Util.map("url", "jdbc:derby:derbyDB"));

    }

    @Test
    public void testWithPeriodic() throws Exception {
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
                Util.map("url", "jdbc:derby:derbyDB"),
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
    public void testLoadJdbcUrlWithSpecialCharWithEmptyPasswordWithAuthentication() throws Exception {
        thrown.expect(QueryExecutionException.class);
        thrown.expectMessage("In config param credentials must be passed both user and password.");
        TestUtil.singleResultFirstColumn(db, "CALL apoc.load.jdbc($url, 'PERSON',[],{credentials:{user:'apoc',password:''}})", Util.map("url","jdbc:derby:derbyDB"));
    }

    @Test
    public void testLoadJdbcUrlWithSpecialCharWithoutPasswordWithAuthentication() throws Exception {
        thrown.expect(QueryExecutionException.class);
        thrown.expectMessage("In config param credentials must be passed both user and password.");
        TestUtil.singleResultFirstColumn(db, "CALL apoc.load.jdbc($url, 'PERSON',[],{credentials:{user:'apoc'}})", Util.map("url","jdbc:derby:derbyDB"));
    }

    @Test
    public void testIterateJDBC() throws Exception {
        final String jdbc = "CALL apoc.load.jdbc($url, 'PERSON',[]) YIELD row RETURN row";
        final String create = "CREATE (p:Person) SET p += row";
        testResult(db, "CALL apoc.periodic.iterate($jdbcQuery, $createQuery, {params: $params})",
                Util.map("params", Util.map("url", "jdbc:derby:derbyDB"), "jdbcQuery", jdbc, "createQuery", create), result -> {
                    Map<String, Object> row = Iterators.single(result);
                    assertEquals(1L, row.get("batches"));
                    assertEquals(1L, row.get("total"));
                });

        testCall(db,
                "MATCH (p:Person) return count(p) as count",
                row -> assertEquals(1L, row.get("count"))
        );
    }

    private void createPersonTableAndData() throws ClassNotFoundException, SQLException, IllegalAccessException, InstantiationException, NoSuchMethodException, InvocationTargetException {
        Class.forName("org.apache.derby.jdbc.EmbeddedDriver").getDeclaredConstructor().newInstance(); // The JDBC specification does not recommend calling newInstance(), but adding a newInstance() call guarantees that Derby will be booted on any JVM. See: http://db.apache.org/derby/docs/10.14/devguide/tdevdvlp20349.html
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
        ps.setDate(2, AbstractJdbcTest.hireDate);
        ps.setTimestamp(3, AbstractJdbcTest.effectiveFromDate);
        ps.setTime(4, AbstractJdbcTest.time);
        ps.setNull(5, Types.DATE);
        int rows = ps.executeUpdate();
        assertEquals(1, rows);
        ResultSet rs = conn.createStatement().executeQuery("SELECT NAME, HIRE_DATE, EFFECTIVE_FROM_DATE, TEST_TIME FROM PERSON");
        assertEquals(true, rs.next());
        assertEquals("John", rs.getString("NAME"));
        Assert.assertEquals(AbstractJdbcTest.hireDate.toLocalDate(), rs.getDate("HIRE_DATE").toLocalDate());
        Assert.assertEquals(AbstractJdbcTest.effectiveFromDate, rs.getTimestamp("EFFECTIVE_FROM_DATE"));
        Assert.assertEquals(AbstractJdbcTest.time, rs.getTime("TEST_TIME"));
        assertEquals(false, rs.next());
        rs.close();
    }

    @Test
    public void testLoadJdbcWrongKey() throws Exception {
        thrown.expect(QueryExecutionException.class);
        thrown.expectMessage("No apoc.jdbc.derbyy.url url specified");
        TestUtil.singleResultFirstColumn(db, "CALL apoc.load.jdbc('derbyy','PERSON')");
    }

}
