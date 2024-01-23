package apoc.load;

import apoc.util.TestUtil;
import apoc.util.Util;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.PostgreSQLContainer;

import java.sql.SQLException;
import java.util.Map;

import static apoc.util.TestUtil.testCall;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertArrayEquals;

public class PostgresJdbcTest extends AbstractJdbcTest {

    // Even if Postgres support the `TIMESTAMP WITH TIMEZONE` type,
    // the JDBC driver doesn't. Please check https://github.com/pgjdbc/pgjdbc/issues/996 and when the issue is closed fix this

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    public static JdbcDatabaseContainer postgress;

    @BeforeClass
    public static void setUp() throws Exception {
        postgress = new PostgreSQLContainer().withInitScript("init_postgres.sql");
        postgress.start();
        TestUtil.registerProcedure(db,Jdbc.class);
        db.executeTransactionally("CALL apoc.load.driver('org.postgresql.Driver')");
    }

    @AfterClass
    public static void tearDown() throws SQLException {
        postgress.stop();
        db.shutdown();
    }

    @Test
    public void testLoadJdbc() throws Exception {
        testCall(db, "CALL apoc.load.jdbc($url,'PERSON',[], $config)", Util.map("url", postgress.getJdbcUrl(),
                "config", Util.map("schema", "test",
                        "credentials", Util.map("user", postgress.getUsername(), "password", postgress.getPassword()))),
                (row) -> assertResult(row));
    }

    @Test
    public void testLoadJdbSelect() throws Exception {
        testCall(db, "CALL apoc.load.jdbc($url,'SELECT * FROM PERSON',[], $config)", Util.map("url", postgress.getJdbcUrl(),
                "config", Util.map("schema", "test",
                        "credentials", Util.map("user", postgress.getUsername(), "password", postgress.getPassword()))),
                (row) -> assertResult(row));
    }

    @Test
    public void testLoadJdbSelectWithArrays() throws Exception {
        testCall(db, "CALL apoc.load.jdbc($url,'SELECT * FROM ARRAY_TABLE',[], $config)", Util.map("url", postgress.getJdbcUrl(),
                        "config", Util.map("schema", "test",
                                "credentials", Util.map("user", postgress.getUsername(), "password", postgress.getPassword()))),
                (result) -> {
                    Map<String, Object> row = (Map<String, Object>)result.get("row");
                    assertEquals("John", row.get("NAME"));
                    int[] intVals = (int[])row.get("INT_VALUES");
                    assertArrayEquals(intVals, new int[]{1, 2, 3});
                    double[] doubleVals = (double[])row.get("DOUBLE_VALUES");
                    assertArrayEquals(doubleVals, new double[]{ 1.0, 2.0, 3.0}, 0.01);
                });
    }

    @Test
    public void testLoadJdbcUpdate() throws Exception {
        testCall(db, "CALL apoc.load.jdbcUpdate($url,'UPDATE PERSON SET \"SURNAME\" = ? WHERE \"NAME\" = ?', ['DOE', 'John'], $config)",
                Util.map("url", postgress.getJdbcUrl(),
                        "config", Util.map("schema", "test",
                                "credentials", Util.map("user", postgress.getUsername(), "password", postgress.getPassword()))),
                (row) -> assertEquals( Util.map("count", 1 ), row.get("row")));
    }

    @Test
    public void testLoadJdbcParams() throws Exception {
        testCall(db, "CALL apoc.load.jdbc($url,'SELECT * FROM PERSON WHERE \"NAME\" = ?',['John'], $config)", //  YIELD row RETURN row
                Util.map("url", postgress.getJdbcUrl(),
                        "config", Util.map("schema", "test",
                                "credentials", Util.map("user", postgress.getUsername(), "password", postgress.getPassword()))),
                (row) -> assertResult(row));
    }
    
}
