package apoc.load;

import apoc.util.TestUtil;
import apoc.util.Util;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.PostgreSQLContainer;

import java.sql.SQLException;

import static apoc.util.TestUtil.isTravis;
import static apoc.util.TestUtil.testCall;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.*;

public class PostgresJdbcTest extends AbstractJdbcTest {

    // Even if Postgres support the `TIMESTAMP WITH TIMEZONE` type,
    // the JDBC driver doesn't. Please check https://github.com/pgjdbc/pgjdbc/issues/996 and when the issue is closed fix this

    private static GraphDatabaseService db;

    public static JdbcDatabaseContainer postgress;

    @BeforeClass
    public static void setUp() throws Exception {
        assumeFalse(isTravis());
        TestUtil.ignoreException(() -> {
            postgress = new PostgreSQLContainer().withInitScript("init_postgres.sql");
            postgress.start();
        },Exception.class);
        assumeNotNull("Postgres container has to exist", postgress);
        assumeTrue("Postgres must be running", postgress.isRunning());
        db = TestUtil.apocGraphDatabaseBuilder().newGraphDatabase();
        TestUtil.registerProcedure(db,Jdbc.class);
        db.execute("CALL apoc.load.driver('org.postgresql.Driver')").close();
    }

    @AfterClass
    public static void tearDown() throws SQLException {
        if (postgress != null) {
            postgress.stop();
            db.shutdown();
        }
    }

    @Test
    public void testLoadJdbc() throws Exception {
        testCall(db, "CALL apoc.load.jdbc({url},'PERSON')", Util.map("url", postgress.getJdbcUrl(),
                "config", Util.map("schema", "test",
                        "credentials", Util.map("user", postgress.getUsername(), "password", postgress.getPassword()))),
                (row) -> assertResult(row));
    }

    @Test
    public void testLoadJdbSelect() throws Exception {
        testCall(db, "CALL apoc.load.jdbc({url},'SELECT * FROM PERSON')", Util.map("url", postgress.getJdbcUrl(),
                "config", Util.map("schema", "test",
                        "credentials", Util.map("user", postgress.getUsername(), "password", postgress.getPassword()))),
                (row) -> assertResult(row));
    }

    @Test
    public void testLoadJdbcUpdate() throws Exception {
        testCall(db, "CALL apoc.load.jdbcUpdate({url},'UPDATE PERSON SET \"SURNAME\" = ? WHERE \"NAME\" = ?', ['DOE', 'John'])",
                Util.map("url", postgress.getJdbcUrl(),
                        "config", Util.map("schema", "test",
                                "credentials", Util.map("user", postgress.getUsername(), "password", postgress.getPassword()))),
                (row) -> assertEquals( Util.map("count", 1 ), row.get("row")));
    }

    @Test
    public void testLoadJdbcParams() throws Exception {
        testCall(db, "CALL apoc.load.jdbc({url},'SELECT * FROM PERSON WHERE \"NAME\" = ?',['John'])", //  YIELD row RETURN row
                Util.map("url", postgress.getJdbcUrl(),
                        "config", Util.map("schema", "test",
                                "credentials", Util.map("user", postgress.getUsername(), "password", postgress.getPassword()))),
                (row) -> assertResult(row));
    }
    
}
