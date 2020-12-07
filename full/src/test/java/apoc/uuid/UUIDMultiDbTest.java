package apoc.uuid;

import apoc.util.Neo4jContainerExtension;
import apoc.util.TestUtil;
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

import static apoc.ApocConfig.APOC_UUID_ENABLED;
import static apoc.ApocConfig.APOC_UUID_ENABLED_DB;
import static apoc.util.TestContainerUtil.createEnterpriseDB;
import static apoc.util.TestUtil.isTravis;
import static apoc.uuid.UuidHandler.NOT_ENABLED_ERROR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeNotNull;

public class UUIDMultiDbTest {

    private static Neo4jContainerExtension neo4jContainer;
    private static Driver driver;
    private static String dbTest = "dbtest";

    @BeforeClass
    public static void setupContainer() {
        assumeFalse(isTravis());
        TestUtil.ignoreException(() -> {
            neo4jContainer = createEnterpriseDB(!TestUtil.isTravis())
                    .withEnv(Map.of(String.format(APOC_UUID_ENABLED_DB, dbTest), "false",
                            APOC_UUID_ENABLED, "true"));
            neo4jContainer.start();
        }, Exception.class);
        assumeNotNull(neo4jContainer);

        driver = GraphDatabase.driver(neo4jContainer.getBoltUrl(), AuthTokens.basic("neo4j", "apoc"));

        try (Session session = driver.session()) {
            session.writeTransaction(tx -> tx.run(String.format("CREATE DATABASE %s;", dbTest)));
        }
    }

    @AfterClass
    public static void bringDownContainer() {
        if (neo4jContainer != null) {
            neo4jContainer.close();
        }
    }

    @Test(expected = RuntimeException.class)
    public void testWithSpecificDatabaseWithUUIDDisabled() throws Exception {

        Session session = driver.session(SessionConfig.forDatabase(dbTest));
        try{
            session.writeTransaction(tx -> tx.run(
                    "CREATE (d:Foo {name:'Test'})-[:WORK]->(l:Bar {name:'Baz'})")
            );

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
                    "CREATE (d:Foo {name:'Test'})-[:WORK]->(l:Bar {name:'Baz'})")
            );

            session.writeTransaction(tx -> tx.run(
                    "CREATE CONSTRAINT ON (foo:Foo) ASSERT foo.uuid IS UNIQUE")
            );

            session.writeTransaction(tx -> tx.run(
                    "CALL apoc.uuid.install('Foo') YIELD label RETURN label")
            );

            String call = "MATCH (n:Foo) RETURN n.uuid as uuid";
            AtomicBoolean nodeHasUUID = new AtomicBoolean(false);
            Consumer<Iterator<Map<String, Object>>> resultConsumer = (result) -> {
                Map<String, Object> r = result.next();
                nodeHasUUID.set(r.get("uuid") != null);
            };

            long timeout = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(5);
            System.out.println("timeout = " + timeout);
            while (!nodeHasUUID.get() || System.currentTimeMillis() > timeout) {
                session.writeTransaction(tx -> {
                    Map<String, Object> p = Collections.<String, Object>emptyMap();
                    resultConsumer.accept(tx.run(call, p).list().stream().map(Record::asMap).collect(Collectors.toList()).iterator());
                    tx.commit();
                    return null;
                });

                if (!nodeHasUUID.get()) {
                    Thread.sleep(100);
                }
            }
            assertTrue(nodeHasUUID.get());
        }
    }
}
