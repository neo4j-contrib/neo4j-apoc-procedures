package apoc.load;

import apoc.ApocConfiguration;
import apoc.util.TestUtil;
import apoc.util.Util;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.testcontainers.containers.CassandraContainer;
import org.testcontainers.couchbase.CouchbaseContainer;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Calendar;
import java.util.Properties;

import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testCall;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeNotNull;
import static org.junit.Assume.assumeTrue;

public class CassandraJdbcTest {

    private GraphDatabaseService db;
    private static java.sql.Date hireDate = new java.sql.Date( new Calendar.Builder().setDate(2017, 04, 25).build().getTimeInMillis() );
    private static java.sql.Timestamp effectiveFromDate = java.sql.Timestamp.from(Instant.now());
    private static java.sql.Time time = java.sql.Time.valueOf("15:37:00");

    public static CassandraContainer cassandra;

    @Before
    public void setUp() throws Exception {
        db = TestUtil.apocGraphDatabaseBuilder().newGraphDatabase();
        ApocConfiguration.initialize((GraphDatabaseAPI)db);
        TestUtil.ignoreException(() -> {
            cassandra = new CassandraContainer();
            cassandra.withInitScript("init_cassandra.cql");
            cassandra.start();
        },Exception.class);
        assumeNotNull("Cassandra container has to exist", cassandra);
        assumeTrue("Cassandra must be running", cassandra.isRunning());
        TestUtil.registerProcedure(db,Jdbc.class);
        db.execute("CALL apoc.load.driver('com.github.adejanovski.cassandra.jdbc.CassandraDriver')").close();
    }

    @After
    public void tearDown() throws SQLException {
        db.shutdown();

        if (cassandra != null) {
            cassandra.stop();
        }
    }

    @Test
    public void testLoadJdbcCassandra() throws Exception {

        testCall(db, "CALL apoc.load.jdbc({url},'person')", Util.map("url", getUrl(),
                "config", Util.map("schema", "test",
                        "credentials", Util.map("user", cassandra.getUsername(), "password", cassandra.getPassword()))),
                (row) -> assertEquals( Util.map("id", 1, "name", "John",
                        "effective_from_date", LocalDateTime.parse("2016-06-22T19:10:25"), "exist", true, "null_date", null, "double_col", 1.2), row.get("row")));
    }

    @Test
    public void testLoadJdbcCassandraSelect() throws Exception {

        testCall(db, "CALL apoc.load.jdbc({url},'SELECT * FROM person')", Util.map("url", getUrl(),
                "config", Util.map("schema", "test",
                        "credentials", Util.map("user", cassandra.getUsername(), "password", cassandra.getPassword()))),
                (row) -> assertEquals( Util.map("id", 1, "name", "John",
                        "effective_from_date", LocalDateTime.parse("2016-06-22T19:10:25"), "exist", true, "null_date", null, "double_col", 1.2), row.get("row")));
    }

    @Test
    public void testLoadJdbcCassandraUpdate() throws Exception {

        testCall(db, "CALL apoc.load.jdbcUpdate({url},'UPDATE person SET name = \\\'John\\\' WHERE id = 1')", Util.map("url", getUrl(),
                "config", Util.map("schema", "test",
                        "credentials", Util.map("user", cassandra.getUsername(), "password", cassandra.getPassword()))),
                (row) -> assertEquals( Util.map("count", 1 ), row.get("row")));
    }

    @Test
    public void testLoadJdbcParamsCassandra() throws Exception {
        testCall(db, "CALL apoc.load.jdbc({url},'SELECT * FROM person WHERE name = ?',['John'])", Util.map("url", getUrl(),
                "config", Util.map("schema", "test",
                        "credentials", Util.map("user", cassandra.getUsername(), "password", cassandra.getPassword()))),
                (row) -> assertEquals( Util.map("id", 1, "name", "John",
                        "effective_from_date", LocalDateTime.parse("2016-06-22T19:10:25"), "exist", true, "null_date", null, "double_col", 1.2), row.get("row")));
    }

    public static String getUrl() {
        return String.format("jdbc:cassandra://%s:%s/playlist", cassandra.getContainerIpAddress(), cassandra.getMappedPort(cassandra.CQL_PORT));
    }
}
