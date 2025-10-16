package apoc.dv;

import apoc.util.Neo4jContainerExtension;
import apoc.util.TestContainerUtil;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.driver.Record;
import org.neo4j.driver.*;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.types.Path;
import org.neo4j.driver.types.Relationship;

import java.io.File;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static apoc.dv.DataVirtualizationCatalogTestUtil.*;
import static apoc.util.ExtendedTestContainerUtil.singleResultFirstColumn;
import static apoc.util.MapUtil.map;
import static apoc.util.TestContainerUtil.*;
import static org.junit.Assert.assertEquals;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.test.assertion.Assert.assertEventually;

public class DataVirtualizationCatalogMultiDbTest {

    private static Neo4jContainerExtension neo4jContainer;
    private static Driver driver;
    private static Session neo4jSession;
    private static Session fooSession;
    private static Session systemSession;

    private static final String DB_FOO = "dbfoo";

    @BeforeClass
    public static void setupContainer() throws Exception {
        neo4jContainer = createEnterpriseDB(
                List.of(TestContainerUtil.ApocPackage.EXTENDED, TestContainerUtil.ApocPackage.CORE), true
        );
        neo4jContainer.start();
        driver = neo4jContainer.getDriver();

        FileUtils.copyFileToDirectory(new File(new URI(FILE_URL).toURL().getPath()), importFolder);
        
        createDatabases();
        createSessions();
    }

    @After
    public void cleanDb() {
        neo4jSession.executeWrite(tx -> tx.run("MATCH (n) DETACH DELETE n;").consume());
        fooSession.executeWrite(tx -> tx.run("MATCH (n) DETACH DELETE n;").consume());
    }

    @Test
    public void testVirtualizeCSVWithDefaultDb() {
        testVirtualizeCSVCommon(DEFAULT_DATABASE_NAME, neo4jSession);
    }

    @Test
    public void testVirtualizeCSVWithSecondaryDb() {
        testVirtualizeCSVCommon(DB_FOO, fooSession);
    }

    @Test
    public void testVirtualizeCSVWithSecondaryDbUsingAnAlias() {
        testVirtualizeCSVCommon("test-alias", fooSession);
    }

    private static void testVirtualizeCSVCommon(String db, Session session) {
        TestContainerUtil.testCall(systemSession, APOC_DV_INSTALL_QUERY,
                        getApocDvInstallParams(db),
                        (row) -> assertCatalogContent(row, CSV_TEST_FILE));

        session.executeRead(tx -> {
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

        String countCustom = "CALL apoc.dv.catalog.show($db) YIELD name RETURN count(*) AS count";
        long dvCount = singleResultFirstColumn(neo4jSession, countCustom, map("db", db));
        assertEquals(1, dvCount);

        session.executeWrite(tx -> tx.run(CREATE_HOOK_QUERY, CREATE_HOOK_PARAMS).consume());

        session.executeRead(tx -> {
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

        testCallEmpty(systemSession, APOC_DV_DROP_QUERY, getApocDvDropParams(db));
    }


    private static void createDatabases() {
        try(Session systemSession = driver.session(SessionConfig.forDatabase("system"))) {
            systemSession.executeWrite(tx -> {
                tx.run("CREATE DATABASE " + DB_FOO + " WAIT;").consume();
                tx.run("CREATE ALIAS `test-alias` FOR DATABASE " + DB_FOO).consume();
                return null;
            });
        }

        try(Session systemSession = driver.session(SessionConfig.forDatabase("system"))) {
            assertEventually(() -> {
                final List<Record> list = systemSession.run("SHOW DATABASES YIELD name, currentStatus")
                        .list();
                return list.stream().allMatch(i -> i.get("currentStatus").asString().equals("online"))
                        && list.stream().map(i -> i.get("name").asString()).toList().contains(DB_FOO);
            }, value -> value, 30L, TimeUnit.SECONDS);
        }
    }

    private static void createSessions() {
        neo4jSession = neo4jContainer.getSession();
        systemSession = driver.session(SessionConfig.forDatabase(SYSTEM_DATABASE_NAME));
        fooSession = driver.session(SessionConfig.forDatabase(DB_FOO));
    }
}
