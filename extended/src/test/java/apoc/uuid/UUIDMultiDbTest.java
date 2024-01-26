package apoc.uuid;

import apoc.util.Neo4jContainerExtension;
import apoc.util.SystemDbTestUtil;
import apoc.util.TestContainerUtil.ApocPackage;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.driver.*;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static apoc.ExtendedApocConfig.APOC_UUID_ENABLED;
import static apoc.ExtendedApocConfig.APOC_UUID_ENABLED_DB;
import static apoc.util.TestContainerUtil.createEnterpriseDB;
import static apoc.uuid.UUIDTestUtils.assertIsUUID;
import static apoc.uuid.UuidHandler.APOC_UUID_REFRESH;
import static apoc.uuid.UuidHandler.NOT_ENABLED_ERROR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.test.assertion.Assert.assertEventually;

public class UUIDMultiDbTest {

    private static Neo4jContainerExtension neo4jContainer;
    private static Driver driver;
    private static Session neo4jSession;
    private static Session dbTestSession;
    private static final String DB_TEST = "dbtest";
    private static final String DB_ENABLED = "dbenabled";
    private static SessionConfig SYS_CONF;

    @BeforeClass
    public static void setupContainer() {
        neo4jContainer = createEnterpriseDB(List.of(ApocPackage.EXTENDED), true)
                .withEnv(String.format(APOC_UUID_ENABLED_DB, DB_TEST), "false")
                .withEnv(APOC_UUID_ENABLED, "true")
                .withEnv(APOC_UUID_REFRESH, "1000");
        neo4jContainer.start();
        driver = neo4jContainer.getDriver();
        SYS_CONF = SessionConfig.forDatabase(SYSTEM_DATABASE_NAME);
        createDatabases();
        createSessions();
    }

    @AfterClass
    public static void bringDownContainer() {
        neo4jContainer.close();
    }

    @After
    public void after() {
        try (Session session = driver.session(SYS_CONF)) {
            Stream.of("neo4j", DB_ENABLED).forEach(
                    db -> session.run("CALL apoc.uuid.dropAll($db)",
                            Map.of("db", db))
            );
        }
    }

    @Test(expected = RuntimeException.class)
    public void testWithSpecificDatabaseWithUUIDDisabled() {
        try {
            dbTestSession.executeWrite(tx -> tx.run("CREATE CONSTRAINT FOR (foo:Foo) REQUIRE foo.uuid IS UNIQUE").consume());
            dbTestSession.executeWrite(tx -> {
                tx.run("CREATE (d:Foo {name:'Test'})-[:WORK]->(l:Bar {name:'Baz'})").consume();
                return tx.run("CALL apoc.uuid.install('Foo', {addToExistingNodes: false }) YIELD label RETURN label").consume();
            });
        } catch (RuntimeException e) {
            String expectedMessage = "Failed to invoke procedure `apoc.uuid.install`: " +
                    "Caused by: java.lang.RuntimeException: " + String.format(NOT_ENABLED_ERROR, DB_TEST);
            assertEquals(expectedMessage, e.getMessage());
            throw e;
        }
    }

    @Test
    public void testWithDefaultDatabaseWithUUIDEnabled() {
        neo4jSession.executeWrite(tx -> tx.run("CREATE CONSTRAINT FOR (foo:Foo) REQUIRE foo.uuid IS UNIQUE").consume());
        neo4jSession.executeWrite(tx -> {
            tx.run("CALL apoc.uuid.install('Foo', {addToExistingNodes: false }) YIELD label RETURN label").consume();
            return tx.run("CREATE (d:Foo {name:'Test'})-[:WORK]->(l:Bar {name:'Baz'})").consume();
        });

        String call = "MATCH (n:Foo) RETURN n.uuid as uuid";
        AtomicBoolean nodeHasUUID = new AtomicBoolean(false);
        Consumer<Iterator<Map<String, Object>>> resultConsumer = (result) -> {
            Map<String, Object> r = result.next();
            nodeHasUUID.set(r.get("uuid") != null);
        };

        long timeout = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(10);
        while (System.currentTimeMillis() < timeout && !nodeHasUUID.get()) {
            neo4jSession.executeWrite(tx -> {
                Map<String, Object> p = Collections.emptyMap();
                resultConsumer.accept(tx.run(call, p).list()
                        .stream()
                        .map(org.neo4j.driver.Record::asMap)
                        .collect(Collectors.toList()).iterator());
                return null;
            });
        }
        assertTrue("UUID not set on node after 5 seconds", nodeHasUUID.get());
    }


    //
    // New procedures tests
    //

    @Test(expected = RuntimeException.class)
    public void createUUIDWithSpecificDbWithUUIDDisabled() {
        try(Session session = driver.session(SYS_CONF)) {
            session.executeWrite(tx -> tx.run(
                    "CALL apoc.uuid.setup('Disabled', $db) YIELD label RETURN label",
                    Map.of("db", DB_TEST)
            ).consume());
            fail("Should fail due to uuid disabled");
        } catch (RuntimeException e) {
            String expectedMessage = "Failed to invoke procedure `apoc.uuid.setup`: " +
                    "Caused by: java.lang.RuntimeException: " + String.format(NOT_ENABLED_ERROR, DB_TEST);
            assertEquals(expectedMessage, e.getMessage());
            throw e;
        }
    }

    @Test
    public void createUUIDWithDefaultDbWithUUIDEnabled() {
        driver.session(SYS_CONF)
                .executeWrite(tx -> tx.run("CALL apoc.uuid.setup('Baz', 'neo4j', {addToExistingNodes: false })").consume());

        // check uuid set
        awaitUuidSet(neo4jSession, "Baz");

        neo4jSession.executeWrite(tx -> tx.run("CREATE (:Baz)").consume());

        assertEventually(() -> {
            Result res = neo4jSession.run("MATCH (n:Baz) RETURN n.uuid AS uuid");
            assertTrue(res.hasNext());
            String uuid = res.single().get("uuid").asString();
            assertIsUUID(uuid);
            return true;
        }, (val) -> val, 10L, TimeUnit.SECONDS);
    }

    @Test
    public void setupUUIDInNotDefaultDbWithUUIDEnabled() throws InterruptedException {
        driver.session(SYS_CONF)
                .executeWrite(tx -> tx.run("CALL apoc.uuid.setup('Another', $db, {addToExistingNodes: false })",
                        Map.of("db", DB_ENABLED)).consume()
                );

        try(Session session = driver.session(SessionConfig.forDatabase(DB_ENABLED))) {
            // check uuid set
            awaitUuidSet(session, "Another");

            session.executeWrite(tx -> tx.run("CREATE (:Another)").consume());

            Result res = session.run("MATCH (n:Another) RETURN n.uuid AS uuid");
            String uuid = res.single().get("uuid").asString();
            assertIsUUID(uuid);
        }

        try(Session session = driver.session()) {
            session.run("CREATE (:Another)");
            // we cannot use assertEventually because the before and after conditions should be the same
            Thread.sleep(2000);

            Value uuid = session.run("MATCH (n:Another) RETURN n.uuid AS uuid")
                    .single()
                    .get("uuid");
            assertTrue(uuid.isNull());
        }
    }

    private static void awaitUuidSet(Session session, String expected) {
        assertEventually(() -> {
            Result res = session.run("CALL apoc.uuid.list");
            assertTrue(res.hasNext());
            String label = res.single().get("label").asString();
            return expected.equals(label);
        }, (val) -> val, SystemDbTestUtil.TIMEOUT, TimeUnit.SECONDS);
    }

    private static void createDatabases() {
        try (Session systemSession = driver.session(SessionConfig.forDatabase("system"))) {
            Stream.of(DB_TEST, DB_ENABLED).forEach(
                    db -> systemSession.executeWrite(
                            tx -> tx.run(String.format("CREATE DATABASE %s WAIT;", db)).consume()
                    ));
        }
    }

    private static void createSessions() {
        neo4jSession = neo4jContainer.getSession();
        dbTestSession = driver.session(SessionConfig.forDatabase(DB_TEST));
    }
}
