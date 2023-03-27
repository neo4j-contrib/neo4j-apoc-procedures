package apoc.uuid;

import apoc.util.Neo4jContainerExtension;
import apoc.util.TestUtil;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.driver.*;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static apoc.ApocConfig.APOC_UUID_ENABLED;
import static apoc.ApocConfig.APOC_UUID_ENABLED_DB;
import static apoc.util.SystemDbTestUtil.TIMEOUT;
import static apoc.util.TestContainerUtil.createEnterpriseDB;
import static apoc.util.TestUtil.isRunningInCI;
import static apoc.uuid.UUIDTestUtils.assertIsUUID;
import static apoc.uuid.UuidHandler.APOC_UUID_REFRESH;
import static apoc.uuid.UuidHandler.NOT_ENABLED_ERROR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeNotNull;
import static org.junit.Assume.assumeTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.test.assertion.Assert.assertEventually;

public class UUIDMultiDbTest {

    private static Neo4jContainerExtension neo4jContainer;
    private static Driver driver;
    private static final String dbTest = "dbtest";
    private static final String dbEnabled = "dbenabled";
    private static SessionConfig SYS_CONF;

    @BeforeClass
    public static void setupContainer() {
        assumeFalse(isRunningInCI());
        TestUtil.ignoreException(() -> {
            neo4jContainer = createEnterpriseDB(true)
                    .withEnv(Map.of(String.format(APOC_UUID_ENABLED_DB, dbTest), "false",
                            APOC_UUID_ENABLED, "true",
                            APOC_UUID_REFRESH, "1000"));
            neo4jContainer.start();
        }, Exception.class);
        assumeNotNull(neo4jContainer);
        assumeTrue("Neo4j Instance should be up-and-running", neo4jContainer.isRunning());

        driver = neo4jContainer.getDriver();
        SYS_CONF = SessionConfig.forDatabase(SYSTEM_DATABASE_NAME);

        try (Session session = driver.session()) {
            Stream.of(dbTest, dbEnabled).forEach(
                    db -> session.run(String.format("CREATE DATABASE %s", db))
            );
        }
    }

    @AfterClass
    public static void bringDownContainer() {
        if (neo4jContainer != null && neo4jContainer.isRunning()) {
            neo4jContainer.close();
        }
    }

    @After
    public void after() {
        try (Session session = driver.session(SYS_CONF)) {
            Stream.of("neo4j", dbEnabled).forEach(
                    db -> session.run("CALL apoc.uuid.dropAll($db)",
                            Map.of("db", db))
            );
        }
    }

    @Test(expected = RuntimeException.class)
    public void testWithSpecificDatabaseWithUUIDDisabled() throws Exception {

        Session session = driver.session(SessionConfig.forDatabase(dbTest));
        try {

            session.writeTransaction(tx -> tx.run(
                    "CREATE CONSTRAINT ON (foo:Foo) ASSERT foo.uuid IS UNIQUE")
            );

            session.writeTransaction(tx -> tx.run(
                    "CALL apoc.uuid.install('Foo') YIELD label RETURN label")
            );

        } catch (RuntimeException e) {
            String expectedMessage = "Failed to invoke procedure `apoc.uuid.install`: " +
                    "Caused by: java.lang.RuntimeException: " + String.format(NOT_ENABLED_ERROR, dbTest);
            assertEquals(expectedMessage, e.getMessage());
            throw e;
        }
    }

    @Test
    public void testWithDefaultDatabaseWithUUIDEnabled() throws InterruptedException {
        try (Session session = driver.session(SessionConfig.forDatabase("neo4j"))) {

            session.writeTransaction(tx -> tx.run(
                    "CREATE CONSTRAINT ON (foo:Foo) ASSERT foo.uuid IS UNIQUE")
            );

            session.writeTransaction(tx -> tx.run(
                    "CALL apoc.uuid.install('Foo') YIELD label RETURN label")
            );
            session.writeTransaction(tx -> tx.run(
                    "CREATE (d:Foo {name:'Test'})-[:WORK]->(l:Bar {name:'Baz'})")
            );

            String call = "MATCH (n:Foo) RETURN n.uuid as uuid";
            AtomicBoolean nodeHasUUID = new AtomicBoolean(false);
            Consumer<Iterator<Map<String, Object>>> resultConsumer = (result) -> {
                Map<String, Object> r = result.next();
                nodeHasUUID.set(r.get("uuid") != null);
            };

            long timeout = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(5);
            while (System.currentTimeMillis() < timeout && !nodeHasUUID.get()) {
                session.writeTransaction(tx -> {
                    Map<String, Object> p = Collections.<String, Object>emptyMap();
                    resultConsumer.accept(tx.run(call, p).list()
                            .stream()
                            .map(Record::asMap)
                            .collect(Collectors.toList()).iterator());
                    tx.commit();
                    return null;
                });
            }
            assertTrue("UUID not set on node after 5 seconds", nodeHasUUID.get());
        }
    }

    //
    // New procedures tests
    //

    @Test(expected = RuntimeException.class)
    public void createUUIDWithSpecificDbWithUUIDDisabled() {
        driver.session(SessionConfig.forDatabase(dbTest))
                .run("CREATE CONSTRAINT ON (foo:Baz) ASSERT foo.uuid IS UNIQUE");

        try(Session session = driver.session(SYS_CONF)) {
            session.run("CALL apoc.uuid.setup('Baz', $db) YIELD label RETURN label",
                    Map.of("db", dbTest)
            );
        } catch (RuntimeException e) {
            String expectedMessage = "Failed to invoke procedure `apoc.uuid.setup`: " +
                    "Caused by: java.lang.RuntimeException: " + String.format(NOT_ENABLED_ERROR, dbTest);
            assertEquals(expectedMessage, e.getMessage());
            throw e;
        }
    }

    @Test
    public void createUUIDWithDefaultDbWithUUIDEnabled() {
        driver.session(SYS_CONF)
                .run("CALL apoc.uuid.setup('Baz')");

        try(Session session = driver.session()) {
            // check uuid set
            awaitUuidSet(session, "Baz");

            session.run("CREATE (:Baz)");

            assertEventually(() -> {
                Result res = session.run("MATCH (n:Baz) RETURN n.uuid AS uuid");
                assertTrue(res.hasNext());
                String uuid = res.single().get("uuid").asString();
                assertIsUUID(uuid);
                return true;
            }, (val) -> val, 10L, TimeUnit.SECONDS);
        }
    }

    @Test
    public void createUUIDInNotDefaultDbWithUUIDEnabled() throws InterruptedException {
        driver.session(SYS_CONF)
                .run("CALL apoc.uuid.setup('Another', $db)",
                        Map.of("db", dbEnabled));

        try(Session session = driver.session(SessionConfig.forDatabase(dbEnabled))) {
            // check uuid set
            awaitUuidSet(session, "Another");

            session.run("CREATE (:Another)");

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
        }, (val) -> val, TIMEOUT, TimeUnit.SECONDS);
    }

}
