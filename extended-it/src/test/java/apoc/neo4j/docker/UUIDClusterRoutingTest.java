package apoc.neo4j.docker;

import apoc.util.Neo4jContainerExtension;
import apoc.util.TestContainerUtil;
import apoc.util.TestcontainersCausalCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.driver.*;
import org.neo4j.internal.helpers.collection.Iterators;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.BiConsumer;

import static apoc.ExtendedApocConfig.APOC_UUID_ENABLED;
import static apoc.util.ExtendedTestContainerUtil.dbIsWriter;
import static apoc.util.ExtendedTestContainerUtil.getBoltAddress;
import static apoc.util.ExtendedTestContainerUtil.getDriverIfNotReplica;
import static apoc.util.SystemDbUtil.PROCEDURE_NOT_ROUTED_ERROR;
import static apoc.util.TestContainerUtil.*;
import static apoc.uuid.UuidHandler.APOC_UUID_REFRESH;

import static java.lang.String.format;
import static org.junit.Assert.*;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;

@Ignore
public class UUIDClusterRoutingTest {
    private static final int NUM_CORES = 4;
    private static TestcontainersCausalCluster cluster;
    private static Session clusterSession;
    private static List<Neo4jContainerExtension> members;

    @BeforeClass
    public static void setupCluster() {
        cluster = TestContainerUtil
                .createEnterpriseCluster(List.of(ApocPackage.EXTENDED), NUM_CORES, 0,
                        Collections.emptyMap(),
                        Map.of("NEO4J_dbms_routing_enabled", "true",
                                APOC_UUID_ENABLED, "true",
                                APOC_UUID_REFRESH, "1000"
                        )
                );

        clusterSession = cluster.getSession();
        members = cluster.getClusterMembers();
        assertEquals(NUM_CORES, members.size());
    }

    @AfterClass
    public static void bringDownCluster() {
        cluster.close();
    }

    @Test
    public void testUuidSetupAllowedOnlyInSysLeaderMember() {
        final String query = "CALL apoc.uuid.setup($label)";
        uuidInSysLeaderMemberCommon(query, PROCEDURE_NOT_ROUTED_ERROR, SYSTEM_DATABASE_NAME,
            (session, label) -> {
                Map<String, Object> params = Map.of("label", label);
                testCall(session, query, params,
                    row -> assertEquals(label, row.get("label"))
                );
                session.executeWrite(tx -> tx.run("CALL apoc.uuid.drop($label)", params).consume());
                }
        );
    }

    @Test
    public void testUuidDropAllowedOnlyInSysLeaderMember() {
        final String query = "CALL apoc.uuid.drop($label)";
        uuidInSysLeaderMemberCommon(query, PROCEDURE_NOT_ROUTED_ERROR, SYSTEM_DATABASE_NAME,
                (session, label) -> testCallEmpty(session, query, Map.of("label", label))
        );
    }

    @Test
    public void testUuidShowAllowedInAllSysLeaderMembers() {
        final String query = "CALL apoc.uuid.show";
        final BiConsumer<Session, String> testUuidShow = (session, name) -> testResult(session, query, Iterators::count);
        uuidInSysLeaderMemberCommon(query, PROCEDURE_NOT_ROUTED_ERROR, SYSTEM_DATABASE_NAME, testUuidShow, true);
    }

    private static void uuidInSysLeaderMemberCommon(String query, String uuidNotRoutedError, String dbName, BiConsumer<Session, String> testUuid) {
        uuidInSysLeaderMemberCommon(query, uuidNotRoutedError, dbName, testUuid, false);
    }

    private static void uuidInSysLeaderMemberCommon(String query, String uuidNotRoutedError, String dbName, BiConsumer<Session, String> testUuid, boolean readOnlyOperation) {
        final List<Neo4jContainerExtension> members = cluster.getClusterMembers();
        assertEquals(NUM_CORES, members.size());
        final String label = UUID.randomUUID().toString();
        clusterSession.executeWrite(tx -> tx.run(format("CREATE CONSTRAINT IF NOT EXISTS FOR (n:`%s`) REQUIRE n.uuid IS UNIQUE", label)).consume());
        for (Neo4jContainerExtension container: members) {
            // we skip READ_REPLICA members with write operations
            // instead, we consider all members with a read only operations
            final Driver driver = readOnlyOperation
                    ? container.getDriver()
                    : getDriverIfNotReplica(container);
            if (driver == null) {
                continue;
            }
            Session session = driver.session(SessionConfig.forDatabase(dbName));
            boolean isWriter = dbIsWriter(dbName, session, getBoltAddress(container));
            if (readOnlyOperation || isWriter) {
                testUuid.accept(session, label);
            } else {
                try {
                    testCall(session, query,
                            Map.of("label", UUID.randomUUID().toString()),
                            row -> fail("Should fail because of non leader UUID addition"));
                } catch (Exception e) {
                    String errorMsg = e.getMessage();
                    assertTrue("The actual message is: " + errorMsg, errorMsg.contains(uuidNotRoutedError));
                }
            }
        }
    }

}
