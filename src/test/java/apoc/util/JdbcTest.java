package apoc.util;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Map;
import java.util.Properties;

import static apoc.util.TestUtil.testCall;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertEquals;

public class JdbcTest {

    private GraphDatabaseService db;
	@Before public void setUp() throws Exception {
	    db = new TestGraphDatabaseFactory().newImpermanentDatabase();
	    ((GraphDatabaseAPI)db).getDependencyResolver().resolveDependency(Procedures.class).register(Json.class);
	}
    @After public void tearDown() {
	    db.shutdown();
    }

    @Test public void testLoadJdbc() throws Exception {
		Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
		Connection conn = DriverManager.getConnection("jdbc:derby:derbyDB;create=true", new Properties());
		conn.createStatement().execute("CREATE TABLE PERSON (name varchar(50));");
		PreparedStatement ps = conn.prepareStatement("INSERT INTO PERSON values(?);");
		ps.setString(1,"John");
		int rows = ps.executeUpdate();
		assertEquals(1,rows);
		ResultSet rs = conn.createStatement().executeQuery("SELECT name FROM PERSON");
		assertEquals(true, rs.next());
		assertEquals("John",rs.getString("name"));
		assertEquals(false, rs.next());
		rs.close();
//		URL url = getClass().getResource("map.json");
		testCall(db, "CALL apoc.util.loadJdbc('jdbc:derby:derbyDB','PERSON') YIELD value RETURN value",
                (row) -> assertEquals(singletonMap("name","John"), row.get("value")) );

		DriverManager.getConnection("jdbc:derby:derbyDB;shutdown=true");
    }
}
