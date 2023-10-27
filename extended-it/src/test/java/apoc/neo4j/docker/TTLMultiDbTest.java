package apoc.neo4j.docker;

import apoc.util.Neo4jContainerExtension;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Record;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static apoc.util.TestContainerUtil.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.test.assertion.Assert.assertEventually;

public class TTLMultiDbTest {

    private static Neo4jContainerExtension neo4jContainer;
    private static Driver driver;
    private static Session neo4jSession;
    private static Session testSession;
    private static Session fooSession;
    private static Session barSession;

    private static final String DB_TEST = "dbtest";
    private static final String DB_FOO = "dbfoo";
    private static final String DB_BAR = "dbbar";

    @BeforeClass
    public static void setupContainer() {
        neo4jContainer = createEnterpriseDB(List.of(ApocPackage.EXTENDED), true)
                .withEnv(Map.of("apoc.ttl.enabled." + DB_TEST, "false",
                        "apoc.ttl.enabled", "true",
                        "apoc.ttl.schedule", "2",
                        "apoc.ttl.schedule." + DB_FOO, "7",
                        "apoc.ttl.limit", "200",
                        "apoc.ttl.limit." + DB_BAR, "2000"));
        neo4jContainer.start();
        driver = neo4jContainer.getDriver();
        createDatabases();
        createSessions();
    }

    @After
    public void cleanDb() {
        neo4jSession.writeTransaction(tx -> tx.run("MATCH (n) DETACH DELETE n;"));
        testSession.writeTransaction(tx -> tx.run("MATCH (n) DETACH DELETE n;"));
        fooSession.writeTransaction(tx -> tx.run("MATCH (n) DETACH DELETE n;"));
        barSession.writeTransaction(tx -> tx.run("MATCH (n) DETACH DELETE n;"));
    }

    @AfterClass
    public static void bringDownContainer() {
        neo4jContainer.close();
    }

    @Test
    public void testWithSpecificDatabaseWithTTLDisabled() throws Exception {
        testCall(testSession, "RETURN apoc.ttl.config() AS value", (row) -> {
            Map<String, Object> value = (Map<String, Object>) row.get("value");
            assertFalse((Boolean) value.get("enabled"));
        });
    }

    @Test
    public void testWithDefaultDatabaseWithTTLEnabled() throws Exception {
        testCall(neo4jSession, "RETURN apoc.ttl.config() AS value", (row) -> {
            Map<String, Object> value = (Map<String, Object>) row.get("value");
            assertTrue((Boolean) value.get("enabled"));
            assertEquals(2L, value.get("schedule"));
            assertEquals(200L, value.get("limit"));
        });
    }

    @Test
    public void testWithDefaultDatabaseAndSpecificDbWithScheduleSpecified() throws Exception {
        testCall(neo4jSession, "RETURN apoc.ttl.config() AS value", (row) -> {
            Map<String, Object> value = (Map<String, Object>) row.get("value");
            assertTrue((Boolean) value.get("enabled"));
            assertEquals(2L, value.get("schedule"));
            assertEquals(200L, value.get("limit"));
        });

        testCall(fooSession, "RETURN apoc.ttl.config() AS value", (row) -> {
            Map<String, Object> value = (Map<String, Object>) row.get("value");
            assertTrue((Boolean) value.get("enabled"));
            assertEquals(7L, value.get("schedule"));
            assertEquals(200L, value.get("limit"));
        });
    }

    @Test
    public void testWithDefaultDatabaseAndSpecificDbWithLimitSpecified() throws Exception {
        testCall(neo4jSession, "RETURN apoc.ttl.config() AS value", (row) -> {
            Map<String, Object> value = (Map<String, Object>) row.get("value");
            assertTrue((Boolean) value.get("enabled"));
            assertEquals(2L, value.get("schedule"));
            assertEquals(200L, value.get("limit"));
        });

        testCall(barSession, "RETURN apoc.ttl.config() AS value", (row) -> {
            Map<String, Object> value = (Map<String, Object>) row.get("value");
            assertTrue((Boolean) value.get("enabled"));
            assertEquals(2L, value.get("schedule"));
            assertEquals(2000L, value.get("limit"));
        });
    }

    private static void createDatabases() {
        try(Session systemSession = driver.session(SessionConfig.forDatabase("system"))) {
            systemSession.writeTransaction(tx -> {
                tx.run("CREATE DATABASE " + DB_TEST + " WAIT;");
                tx.run("CREATE DATABASE " + DB_FOO + " WAIT;");
                return tx.run("CREATE DATABASE " + DB_BAR + " WAIT;");
            });
        }

        try(Session systemSession = driver.session(SessionConfig.forDatabase("system"))) {
            assertEventually(() -> {
                final List<Record> list = systemSession.run("SHOW DATABASES YIELD name, currentStatus")
                        .list();
                return list.stream().allMatch(i -> i.get("currentStatus").asString().equals("online"))
                        && list.stream().map(i -> i.get("name").asString()).toList().containsAll(List.of(DB_TEST , DB_FOO , DB_BAR));
            }, value -> value, 30L, TimeUnit.SECONDS);
        }
    }

    private static void createSessions() {
        neo4jSession = neo4jContainer.getSession();
        testSession = driver.session(SessionConfig.forDatabase(DB_TEST));
        fooSession = driver.session(SessionConfig.forDatabase(DB_FOO));
        barSession = driver.session(SessionConfig.forDatabase(DB_BAR));
    }
}
