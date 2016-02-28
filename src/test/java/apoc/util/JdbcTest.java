package apoc.util;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.sql.*;
import java.util.Properties;

import static apoc.util.TestUtil.testCall;
import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertEquals;

public class JdbcTest {

    private GraphDatabaseService db;

    @Before
    public void setUp() throws Exception {
        db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        TestUtil.registerProcedure(db,Jdbc.class);
    }

    @After
    public void tearDown() {
        db.shutdown();
    }

    @Test
    public void testLoadJdbc() throws Exception {
        createPersonTableAndData();
        testCall(db, "CALL apoc.util.loadJdbc('jdbc:derby:derbyDB','PERSON')", //  YIELD row RETURN row
                (row) -> assertEquals(singletonMap("NAME", "John"), row.get("row")));

//		DriverManager.getConnection("jdbc:derby:derbyDB;shutdown=true");
    }

    private void createPersonTableAndData() throws ClassNotFoundException, SQLException {
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
