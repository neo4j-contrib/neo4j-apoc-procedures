package apoc.uuid;

import apoc.util.Neo4jContainerExtension;
import apoc.util.TestContainerUtil.ApocPackage;
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

import static apoc.ExtendedApocConfig.APOC_UUID_ENABLED;
import static apoc.ExtendedApocConfig.APOC_UUID_ENABLED_DB;
import static apoc.util.TestContainerUtil.createEnterpriseDB;
import static apoc.uuid.UuidHandler.NOT_ENABLED_ERROR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class UUIDMultiDbTest {

    private static Neo4jContainerExtension neo4jContainer;
    private static Driver driver;
    private static Session neo4jSession;
    private static Session dbTestSession;
    private static final String DB_TEST = "dbtest";

    @BeforeClass
    public static void setupContainer() {
        neo4jContainer = createEnterpriseDB(List.of(ApocPackage.EXTENDED), true)
                .withEnv(String.format(APOC_UUID_ENABLED_DB, DB_TEST), "false")
                .withEnv(APOC_UUID_ENABLED, "true");
        neo4jContainer.start();
        driver = neo4jContainer.getDriver();
        createDatabases();
        createSessions();
    }

    @AfterClass
    public static void bringDownContainer() {
        neo4jContainer.close();
    }

    @Test(expected = RuntimeException.class)
    public void testWithSpecificDatabaseWithUUIDDisabled() {
        try {
            dbTestSession.writeTransaction(tx -> tx.run("CREATE CONSTRAINT FOR (foo:Foo) REQUIRE foo.uuid IS UNIQUE"));
            dbTestSession.writeTransaction(tx -> {
                tx.run("CREATE (d:Foo {name:'Test'})-[:WORK]->(l:Bar {name:'Baz'})");
                return tx.run("CALL apoc.uuid.install('Foo', {addToExistingNodes: false }) YIELD label RETURN label");
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
        neo4jSession.writeTransaction(tx -> tx.run("CREATE CONSTRAINT FOR (foo:Foo) REQUIRE foo.uuid IS UNIQUE"));
        neo4jSession.writeTransaction(tx -> {
            tx.run("CALL apoc.uuid.install('Foo', {addToExistingNodes: false }) YIELD label RETURN label");
            return tx.run("CREATE (d:Foo {name:'Test'})-[:WORK]->(l:Bar {name:'Baz'})");
        });

        String call = "MATCH (n:Foo) RETURN n.uuid as uuid";
        AtomicBoolean nodeHasUUID = new AtomicBoolean(false);
        Consumer<Iterator<Map<String, Object>>> resultConsumer = (result) -> {
            Map<String, Object> r = result.next();
            nodeHasUUID.set(r.get("uuid") != null);
        };

        long timeout = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(5);
        while (System.currentTimeMillis() < timeout && !nodeHasUUID.get()) {
            neo4jSession.writeTransaction(tx -> {
                Map<String, Object> p = Collections.<String, Object>emptyMap();
                resultConsumer.accept(tx.run(call, p).list()
                        .stream()
                        .map(org.neo4j.driver.Record::asMap)
                        .collect(Collectors.toList()).iterator());
                tx.commit();
                return null;
            });
        }
        assertTrue("UUID not set on node after 5 seconds", nodeHasUUID.get());
    }

    private static void createDatabases() {
        try (Session systemSession = driver.session(SessionConfig.forDatabase("system"))) {
            systemSession.writeTransaction(tx -> tx.run(String.format("CREATE DATABASE %s WAIT;", DB_TEST)));
        }
    }

    private static void createSessions() {
        neo4jSession = neo4jContainer.getSession();
        dbTestSession = driver.session(SessionConfig.forDatabase(DB_TEST));
    }
}
