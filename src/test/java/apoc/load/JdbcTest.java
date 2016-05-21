package apoc.load;

import apoc.ApocConfiguration;
import apoc.load.Jdbc;
import apoc.util.TestUtil;
import org.junit.*;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.sql.*;
import java.util.Properties;

import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testCall;
import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertEquals;

public class JdbcTest {

    private static GraphDatabaseService db;

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
                (row) -> assertEquals(singletonMap("NAME", "John"), row.get("row")));
    }
    @Test
    public void testLoadJdbcSelect() throws Exception {
        testCall(db, "CALL apoc.load.jdbc('jdbc:derby:derbyDB','SELECT * FROM PERSON')",
                (row) -> assertEquals(singletonMap("NAME", "John"), row.get("row")));
    }

    @Test
    public void testLoadJdbcParams() throws Exception {
        testCall(db, "CALL apoc.load.jdbcParams('jdbc:derby:derbyDB','SELECT * FROM PERSON WHERE NAME = ?',['John'])", //  YIELD row RETURN row
                (row) -> assertEquals(singletonMap("NAME", "John"), row.get("row")));
    }

    @Test
    public void testLoadJdbcKey() throws Exception {
        testCall(db, "CALL apoc.load.jdbc('derby','PERSON')",
                (row) -> assertEquals(singletonMap("NAME", "John"), row.get("row")));
    }

    private static void createPersonTableAndData() throws ClassNotFoundException, SQLException {
        Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
        Connection conn = DriverManager.getConnection("jdbc:derby:derbyDB;create=true", new Properties());
        try { conn.createStatement().execute("DROP TABLE PERSON"); } catch (SQLException se) {/*ignore*/}
        conn.createStatement().execute("CREATE TABLE PERSON (NAME varchar(50))");
        PreparedStatement ps = conn.prepareStatement("INSERT INTO PERSON values(?)");
        ps.setString(1, "John");
        int rows = ps.executeUpdate();
        assertEquals(1, rows);
        ResultSet rs = conn.createStatement().executeQuery("SELECT NAME FROM PERSON");
        assertEquals(true, rs.next());
        assertEquals("John", rs.getString("NAME"));
        assertEquals(false, rs.next());
        rs.close();
    }
}
