package apoc.dv;

import apoc.util.Neo4jContainerExtension;
import apoc.util.TestContainerUtil;
import apoc.util.TestcontainersCausalCluster;
import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.types.Path;
import org.neo4j.driver.types.Relationship;

import java.io.File;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static apoc.dv.DataVirtualizationCatalogTestUtil.*;
import static apoc.util.ExtendedTestContainerUtil.dbIsWriter;
import static apoc.util.ExtendedTestContainerUtil.getBoltAddress;
import static apoc.util.ExtendedTestContainerUtil.getDriverIfNotReplica;
import static apoc.util.MapUtil.map;
import static apoc.util.SystemDbUtil.PROCEDURE_NOT_ROUTED_ERROR;
import static apoc.util.TestContainerUtil.importFolder;
import static apoc.util.TestContainerUtil.testCall;
import static apoc.util.TestContainerUtil.testCallEmpty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;


public class DataVirtualizationCatalogClusterRoutingTest {
    private static final int NUM_CORES = 3;
    private static TestcontainersCausalCluster cluster;
    private static Session clusterSession;
    private static List<Neo4jContainerExtension> members;

    @BeforeClass
    public static void setupCluster() throws Exception {
        cluster = TestContainerUtil
                .createEnterpriseCluster(List.of(TestContainerUtil.ApocPackage.EXTENDED, TestContainerUtil.ApocPackage.CORE), NUM_CORES, 0,
                        Collections.emptyMap(),
                        Map.of("NEO4J_dbms_routing_enabled", "true")
                );
        clusterSession = cluster.getSession();
        members = cluster.getClusterMembers();
        FileUtils.copyFileToDirectory(new File(new URI(FILE_URL).toURL().getPath()), importFolder);
        assertEquals(NUM_CORES, members.size());
    }

    @AfterClass
    public static void bringDownCluster() {
        cluster.close();
    }

    @Test
    public void testVirtualizeCSV() {
        dvInSysLeaderMemberCommon(PROCEDURE_NOT_ROUTED_ERROR, SYSTEM_DATABASE_NAME,
                (session) -> testCall(session, APOC_DV_INSTALL_QUERY,
                        APOC_DV_INSTALL_PARAMS(DEFAULT_DATABASE_NAME),
                        (row) -> assertCatalogContent(row, CSV_TEST_FILE)), APOC_DV_INSTALL_PARAMS(DEFAULT_DATABASE_NAME)
        );

        clusterSession.executeRead(tx -> {
                    final Result result = tx.run(APOC_DV_QUERY,
                            Map.of(NAME_KEY, CSV_NAME_VALUE,
                                    APOC_DV_QUERY_PARAMS_KEY, APOC_DV_QUERY_PARAMS,
                                    CONFIG_KEY, CONFIG_VALUE)
                    );

                    Node node = result.single().get(NODE_KEY).asNode();
                    assertEquals(NAME_VALUE, node.get(NAME_KEY).asString());
                    assertEquals(AGE_VALUE, node.get(AGE_KEY).asString());
                    assertEquals(List.of(LABELS_VALUE), node.labels());

                    return result.consume();
                }
        );

        clusterSession.executeWrite(tx -> tx.run(CREATE_HOOK_QUERY, CREATE_HOOK_PARAMS).consume());

        clusterSession.executeRead(tx -> {
                    final Result result = tx.run(APOC_DV_QUERY_AND_LINK_QUERY,
                            map(NAME_KEY, CSV_NAME_VALUE, APOC_DV_QUERY_PARAMS_KEY, APOC_DV_QUERY_PARAMS, RELTYPE_KEY, RELTYPE_VALUE, CONFIG_KEY, CONFIG_VALUE)
                    );

                    Path path = result.single().get("path").asPath();
                    Node node = path.end();
                    assertEquals(NAME_VALUE, node.get(NAME_KEY).asString());
                    assertEquals(AGE_VALUE, node.get(AGE_KEY).asString());
                    assertEquals(List.of(LABELS_VALUE), node.labels());

                    Node hook = path.start();
                    assertEquals(HOOK_NODE_NAME_VALUE, hook.get(NAME_KEY).asString());
                    assertEquals(List.of("Hook"), hook.labels());

                    Relationship relationship = path.relationships().iterator().next();
                    assertEquals(hook.elementId(), relationship.startNodeElementId());
                    assertEquals(node.elementId(), relationship.endNodeElementId());
                    assertEquals(RELTYPE_VALUE, relationship.type());

                    return result.consume();
                }
        );

        dvInSysLeaderMemberCommon(PROCEDURE_NOT_ROUTED_ERROR, SYSTEM_DATABASE_NAME,
                (session) -> testCallEmpty(session, APOC_DV_DROP_QUERY,
                            APOC_DV_DROP_PARAMS(DEFAULT_DATABASE_NAME)), APOC_DV_DROP_PARAMS(DEFAULT_DATABASE_NAME)
        );

    }

    private static void dvInSysLeaderMemberCommon(String uuidNotRoutedError, String dbName, Consumer<Session> testDv, Map<String, Object> params) {
        dvInSysLeaderMemberCommon(uuidNotRoutedError, dbName, testDv, false, params);
    }

    private static void dvInSysLeaderMemberCommon(String uuidNotRoutedError, String dbName, Consumer<Session> testDv, boolean readOnlyOperation, Map<String, Object> params) {
        final List<Neo4jContainerExtension> members = cluster.getClusterMembers();
        assertEquals(NUM_CORES, members.size());
        boolean writeExecuted = false;
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
            if (isWriter) {
                testDv.accept(session);
                writeExecuted = true;
            } else {
                try {
                    testDv.accept(session);
                    fail("Should fail because of non leader Data Virtualization addition");
                } catch (Exception e) {
                    String errorMsg = e.getMessage();
                    assertTrue("The actual message is: " + errorMsg, errorMsg.contains(uuidNotRoutedError));
                }
            }
        }
        assertTrue(writeExecuted);
    }
}
