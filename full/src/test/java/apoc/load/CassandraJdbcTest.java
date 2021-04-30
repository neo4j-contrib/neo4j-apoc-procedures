package apoc.load;

import apoc.util.TestUtil;
import apoc.util.Util;
import org.hamcrest.Matchers;
import org.hamcrest.collection.IsMapContaining;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;
import org.testcontainers.containers.CassandraContainer;

import java.sql.SQLException;
import java.util.Map;

import static apoc.util.TestUtil.isRunningInCI;
import static apoc.util.TestUtil.testCall;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assume.*;

public class CassandraJdbcTest extends AbstractJdbcTest {

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    public static CassandraContainer cassandra;

    @BeforeClass
    public static void setUp() throws Exception {
        assumeFalse(isRunningInCI());
        TestUtil.ignoreException(() -> {
            cassandra = new CassandraContainer();
            cassandra.withInitScript("init_cassandra.cql");
            cassandra.start();
        },Exception.class);
        assumeNotNull("Cassandra container has to exist", cassandra);
        assumeTrue("Cassandra must be running", cassandra.isRunning());

        TestUtil.registerProcedure(db, Jdbc.class);
        db.executeTransactionally("CALL apoc.load.driver('com.github.adejanovski.cassandra.jdbc.CassandraDriver')");
    }

    @AfterClass
    public static void tearDown() throws SQLException {
        if (cassandra != null) {
            cassandra.stop();
        }
    }

    @Test
    public void testLoadJdbc() throws Exception {
        testCall(db, "CALL apoc.load.jdbc($url,'\"PERSON\"')", Util.map("url", getUrl(),
                "config", Util.map("schema", "test",
                        "credentials", Util.map("user", cassandra.getUsername(), "password", cassandra.getPassword()))),
                (row) -> assertResult(row));
    }

    @Test
    public void testLoadJdbcSelect() throws Exception {
        testCall(db, "CALL apoc.load.jdbc($url,'SELECT * FROM \"PERSON\"')",
                Util.map("url", getUrl(),
                        "config", Util.map("schema", "test", "credentials", Util.map("user", cassandra.getUsername(), "password", cassandra.getPassword()))
                ),
                (row) -> assertResult(row));
    }

    @Test
    public void testLoadJdbcUpdate() throws Exception {
        TestUtil.singleResultFirstColumn(db, "CALL apoc.load.jdbcUpdate($url,'UPDATE \"PERSON\" SET \"SURNAME\" = ? WHERE \"NAME\" = ?', ['DOE', 'John'])",
                Util.map("url", getUrl(),
                    "config", Util.map("schema", "test","credentials", Util.map("user", cassandra.getUsername(), "password", cassandra.getPassword()))
                ));
        testCall(db, "CALL apoc.load.jdbc($url,'SELECT * FROM \"PERSON\" WHERE \"NAME\" = ?', ['John'])",
                Util.map("url", getUrl(),
                        "config", Util.map("schema", "test", "credentials", Util.map("user", cassandra.getUsername(), "password", cassandra.getPassword()))
                ),
                (r) -> {
                    Map<String, Object> row = (Map<String, Object>) r.get("row");
                    assertThat(row, Matchers.allOf(
                            IsMapContaining.hasEntry("NAME", "John") ,
                            IsMapContaining.hasEntry("SURNAME", "DOE"), // FIXME: it seems that cassandra is not updated via first statment in this method
                            IsMapContaining.hasEntry("EFFECTIVE_FROM_DATE", (Object)effectiveFromDate.toLocalDateTime())
                            ));
                });
    }

    @Test
    public void testLoadJdbcParams() throws Exception {
        testCall(db, "CALL apoc.load.jdbc($url,'SELECT * FROM \"PERSON\" WHERE \"NAME\" = ?', ['John'])",
                Util.map("url", getUrl(),
                        "config", Util.map("schema", "test", "credentials", Util.map("user", cassandra.getUsername(), "password", cassandra.getPassword()))
                ),
                (row) -> assertResult(row));
    }

    public static String getUrl() {
        return String.format("jdbc:cassandra://%s:%s/apoc_schema", cassandra.getContainerIpAddress(), cassandra.getMappedPort(cassandra.CQL_PORT));
    }

    @Override
    public void assertResult(Map<String, Object> row) {
        Map<String, Object> expected = Util.map("NAME", "John", "SURNAME", null, "EFFECTIVE_FROM_DATE",
                effectiveFromDate.toLocalDateTime());
        assertEquals(expected, row.get("row"));
    }
}
