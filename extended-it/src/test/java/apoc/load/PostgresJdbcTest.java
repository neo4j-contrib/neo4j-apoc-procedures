package apoc.load;

import apoc.periodic.Periodic;
import apoc.text.Strings;
import apoc.util.TestUtil;
import apoc.util.Util;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.graphdb.Result;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.PostgreSQLContainer;

import java.sql.SQLException;
import java.util.Map;

import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testResult;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

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
        TestUtil.registerProcedure(db,Jdbc.class, Periodic.class, Strings.class);
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

    @Test
    public void testIssue4141PeriodicIterateWithJdbc() throws Exception {
        var config = Util.map("url", postgress.getJdbcUrl(),
                "config", Util.map("schema", "test",
                        "credentials", Util.map("user", postgress.getUsername(), "password", postgress.getPassword())));

        String query = "WITH range(0, 100) as list UNWIND list as l CREATE (n:MyNode{id: l})";

        db.executeTransactionally(query, Map.of(), Result::resultAsString);

        // Redundant, only to reproduce issue 4141
        query = "CALL apoc.load.driver(\"org.postgresql.Driver\")";

        db.executeTransactionally(query, Map.of(), Result::resultAsString);

        // To replicate the 4141 issue case,
        // we cannot use container.getJdbcUrl() because it does not provide the url with username and password
        String jdbUrl = getUrl(postgress);
        
        query = """
                CALL apoc.periodic.iterate(
                    "MATCH (n:MyNode) return n",
                    "WITH n, apoc.text.format('insert into nodes (my_id) values (\\\\\\'%d\\\\\\')',[n.id]) AS sql CALL apoc.load.jdbcUpdate('$url',sql) YIELD row AS row2 return row2,n",
                    {batchsize: 10,parallel: true})
                yield operations
                """.replace("$url", jdbUrl);

        // check that periodic iterate doesn't throw errors
        testResult(db, query, config, this::assertPeriodicIterate);

        assertPgStatActivityHasOnlyActiveState();
    }

    private static void assertPgStatActivityHasOnlyActiveState() throws Exception {
        // connect to postgres and execute the query `select state from pg_stat_activity`
        String psql = postgress.execInContainer(
                "psql", "postgresql://test:test@localhost/test", "-c", "select state from pg_stat_activity;")
                .toString();
        
        assertTrue("Current pg_stat_activity is: " + psql, psql.contains("active"));
        
        // the result without the https://github.com/neo4j-contrib/neo4j-apoc-procedures/issues/4141 change
        // is not deterministic, can be `too many clients already` or (not very often) `idle`
        assertFalse("Current pg_stat_activity is: " + psql,
                psql.contains("too many clients already") || psql.contains("idle"));

    }

    private void assertPeriodicIterate(Result result) {
        Map<String, Object> res = result.next();
        Map<String, Object> operations = (Map<String, Object>) res.get("operations");

        long failed = (Long) operations.get("failed");
        assertEquals(0L, failed);

        long committed = (Long) operations.get("committed");
        assertEquals(101L, committed);
        
        assertFalse(result.hasNext());
    }

    private static String getUrl(JdbcDatabaseContainer container) {
        return String.format(
                "jdbc:postgresql://%s:%s@%s:%s/%s?loggerLevel=OFF",
                container.getUsername(),
                container.getPassword(),
                container.getContainerIpAddress(),
                container.getMappedPort(5432),
                container.getDatabaseName()
        );
    }
}
