package apoc.ttl;

import apoc.util.Neo4jContainerExtension;
import apoc.util.TestContainerUtil;
import apoc.util.TestUtil;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;

import java.util.Map;

import static apoc.util.TestContainerUtil.createEnterpriseDB;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static apoc.util.TestUtil.isRunningInCI;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeNotNull;
import static org.junit.Assume.assumeTrue;

public class TTLMultiDbTest {

    private static Neo4jContainerExtension neo4jContainer;
    private static Driver driver;

    private static final String DB_NEO4J = "neo4j";
    private static final String DB_TEST = "dbtest";
    private static final String DB_FOO = "dbfoo";
    private static final String DB_BAR = "dbbar";

    @BeforeClass
    public static void setupContainer() {
        assumeFalse(isRunningInCI());
        TestUtil.ignoreException(() -> {
            neo4jContainer = createEnterpriseDB(!TestUtil.isRunningInCI())
                    .withEnv(Map.of("apoc.ttl.enabled." + DB_TEST, "false",
                            "apoc.ttl.enabled", "true",
                            "apoc.ttl.schedule", "2",
                            "apoc.ttl.schedule." + DB_FOO, "7",
                            "apoc.ttl.limit", "200",
                            "apoc.ttl.limit." + DB_BAR, "2000"));
            neo4jContainer.start();
        }, Exception.class);
        assumeNotNull(neo4jContainer);
        assumeTrue("Neo4j Instance should be up-and-running", neo4jContainer.isRunning());

        driver = GraphDatabase.driver(neo4jContainer.getBoltUrl(), AuthTokens.basic("neo4j", "apoc"));

        try (Session session = driver.session()) {
            session.writeTransaction(tx -> tx.run("CREATE DATABASE " + DB_TEST + ";"));
            session.writeTransaction(tx -> tx.run("CREATE DATABASE " + DB_FOO + ";"));
            session.writeTransaction(tx -> tx.run("CREATE DATABASE " + DB_BAR + ";"));
        }

    }

    @After
    public void cleanDb() {
        try (Session session = driver.session()) {
            session.writeTransaction(tx -> tx.run("MATCH (n) DETACH DELETE n;"));
        }
    }

    @AfterClass
    public static void bringDownContainer() {
        if (neo4jContainer != null && neo4jContainer.isRunning()) {
            neo4jContainer.close();
        }
    }

    @Test
    public void testWithSpecificDatabaseWithTTLDisabled() throws Exception {
        try (Session session = driver.session(SessionConfig.forDatabase(DB_TEST))) {
            TestContainerUtil.testCall(
                    session,
                    "RETURN apoc.ttl.config() AS value",
                    (row) -> {
                        Map<String, Object> value = (Map<String, Object>) row.get("value");
                        assertFalse((Boolean) value.get("enabled"));
                    });
        }
    }

    @Test
    public void testWithDefaultDatabaseWithTTLEnabled() throws Exception {

        try (Session session = driver.session(SessionConfig.forDatabase(DB_NEO4J))) {
            TestContainerUtil.testCall(
                    session,
                    "RETURN apoc.ttl.config() AS value",
                    (row) -> {
                        Map<String, Object> value = (Map<String, Object>) row.get("value");
                        assertTrue((Boolean) value.get("enabled"));
                        assertEquals(2L, value.get("schedule"));
                        assertEquals(200L, value.get("limit"));
                    });
        }
    }

    @Test
    public void testWithDefaultDatabaseAndSpecificDbWithScheduleSpecified() throws Exception {
        try (Session session = driver.session(SessionConfig.forDatabase(DB_NEO4J))) {
            TestContainerUtil.testCall(
                    session,
                    "RETURN apoc.ttl.config() AS value",
                    (row) -> {
                        Map<String, Object> value = (Map<String, Object>) row.get("value");
                        assertTrue((Boolean) value.get("enabled"));
                        assertEquals(2L, value.get("schedule"));
                        assertEquals(200L, value.get("limit"));
                    });
        }

        try (Session session = driver.session(SessionConfig.forDatabase(DB_FOO))) {
            TestContainerUtil.testCall(
                    session,
                    "RETURN apoc.ttl.config() AS value",
                    (row) -> {
                        Map<String, Object> value = (Map<String, Object>) row.get("value");
                        assertTrue((Boolean) value.get("enabled"));
                        assertEquals(7L, value.get("schedule"));
                        assertEquals(200L, value.get("limit"));
                    });
        }
    }

    @Test
    public void testWithDefaultDatabaseAndSpecificDbWithLimitSpecified() throws Exception {

        try (Session session = driver.session(SessionConfig.forDatabase(DB_NEO4J))) {
            TestContainerUtil.testCall(
                    session,
                    "RETURN apoc.ttl.config() AS value",
                    (row) -> {
                        Map<String, Object> value = (Map<String, Object>) row.get("value");
                        assertTrue((Boolean) value.get("enabled"));
                        assertEquals(2L, value.get("schedule"));
                        assertEquals(200L, value.get("limit"));
                    });
        }

        try (Session session = driver.session(SessionConfig.forDatabase(DB_BAR))) {
            TestContainerUtil.testCall(
                    session,
                    "RETURN apoc.ttl.config() AS value",
                    (row) -> {
                        Map<String, Object> value = (Map<String, Object>) row.get("value");
                        assertTrue((Boolean) value.get("enabled"));
                        assertEquals(2L, value.get("schedule"));
                        assertEquals(2000L, value.get("limit"));
                    });
        }
    }
}
