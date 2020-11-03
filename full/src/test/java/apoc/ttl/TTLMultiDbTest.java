package apoc.ttl;

import apoc.util.*;
import org.junit.*;
import org.neo4j.driver.*;

import java.util.List;
import java.util.Map;

import static apoc.util.TestContainerUtil.createEnterpriseDB;
import static apoc.util.TestUtil.isTravis;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeNotNull;

public class TTLMultiDbTest {

    private static Neo4jContainerExtension neo4jContainer;
    private static Driver driver;

    private static final String DB_NEO4J = "neo4j";
    private static final String DB_TEST = "dbtest";
    private static final String DB_FOO = "dbfoo";
    private static final String DB_BAR = "dbbar";

    @BeforeClass
    public static void setupContainer() {
        assumeFalse(isTravis());
        TestUtil.ignoreException(() -> {
            neo4jContainer = createEnterpriseDB(!TestUtil.isTravis())
                    .withEnv(Map.of("apoc.ttl.enabled." + DB_TEST, "false",
                            "apoc.ttl.enabled", "true",
                            "apoc.ttl.schedule", "2",
                            "apoc.ttl.schedule." + DB_FOO, "7",
                            "apoc.ttl.limit", "200",
                            "apoc.ttl.limit." + DB_BAR, "1000"));
            neo4jContainer.start();
        }, Exception.class);
        assumeNotNull(neo4jContainer);

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
        if (neo4jContainer != null) {
            neo4jContainer.close();
        }
    }

    @Test
    public void testWithSpecificDatabaseWithTTLDisabled() throws Exception {

        try (Session session = driver.session(SessionConfig.forDatabase(DB_TEST))) {
            session.writeTransaction(tx -> tx.run(
                    "UNWIND range(1,100) as range CREATE (n:Foo {id: range}) WITH n CALL apoc.ttl.expireIn(n,500,'ms') RETURN count(*)")
            );

            TestContainerUtil.testCall(
                    session,
                    "MATCH (n:Foo) RETURN collect(n) as row",
                    (row) -> assertEquals(100, ((List) row.get("row")).size()));

            Thread.sleep(4000);

            TestContainerUtil.testCall(
                    session,
                    "MATCH (n:Foo) RETURN collect(n) as row",
                    (row) -> assertEquals(100, ((List) row.get("row")).size()));
        }
    }

    @Test
    public void testWithDefaultDatabaseWithTTLEnabled() throws Exception {

        try (Session session = driver.session(SessionConfig.forDatabase(DB_NEO4J))) {
            session.writeTransaction(tx -> tx.run(
                    "UNWIND range(1,100) as range CREATE (n:Foo {id: range}) WITH n CALL apoc.ttl.expireIn(n,500,'ms') RETURN count(*)")
            );

            TestContainerUtil.testCall(
                    session,
                    "MATCH (n:Foo) RETURN collect(n) as row",
                    (row) -> assertEquals(100, ((List) row.get("row")).size()));

            Thread.sleep(4000);

            TestContainerUtil.testCall(
                    session,
                    "MATCH (n:Foo) RETURN collect(n) as row",
                    (row) -> assertEquals(0, ((List) row.get("row")).size()));
        }
    }

    @Test
    public void testWithDefaultDatabaseAndSpecificDbWithScheduleSpecified() throws Exception {

        try (Session session = driver.session(SessionConfig.forDatabase(DB_NEO4J))) {
            session.writeTransaction(tx -> tx.run(
                    "UNWIND range(1,100) as range CREATE (n:Foo {id: range}) WITH n CALL apoc.ttl.expireIn(n,500,'ms') RETURN count(*)")
            );

            TestContainerUtil.testCall(
                    session,
                    "MATCH (n:Foo) RETURN collect(n) as row",
                    (row) -> assertEquals(100, ((List) row.get("row")).size()));

            Thread.sleep(2000);

            TestContainerUtil.testCall(
                    session,
                    "MATCH (n:Foo) RETURN collect(n) as row",
                    (row) -> assertEquals(0, ((List) row.get("row")).size()));
        }

        try (Session session = driver.session(SessionConfig.forDatabase(DB_FOO))) {
            session.writeTransaction(tx -> tx.run(
                    "UNWIND range(1,100) as range CREATE (n:Foo {id: range}) WITH n CALL apoc.ttl.expireIn(n,500,'ms') RETURN count(*)")
            );

            TestContainerUtil.testCall(
                    session,
                    "MATCH (n:Foo) RETURN collect(n) as row",
                    (row) -> assertEquals(100, ((List) row.get("row")).size()));

            Thread.sleep(2000);

            TestContainerUtil.testCall(
                    session,
                    "MATCH (n:Foo) RETURN collect(n) as row",
                    (row) -> assertEquals(100, ((List) row.get("row")).size()));
        }
    }

    @Test
    public void testWithDefaultDatabaseAndSpecificDbWithLimitSpecified() throws Exception {

        try (Session session = driver.session(SessionConfig.forDatabase(DB_NEO4J))) {
            session.writeTransaction(tx -> tx.run(
                    "UNWIND range(1,2000) as range CREATE (n:Foo {id: range}) WITH n CALL apoc.ttl.expireIn(n,500,'ms') RETURN count(*)")
            );

            TestContainerUtil.testCall(
                    session,
                    "MATCH (n:Foo) RETURN collect(n) as row",
                    (row) -> assertEquals(2000, ((List) row.get("row")).size()));

            Thread.sleep(2000);

            TestContainerUtil.testCall(
                    session,
                    "MATCH (n:Foo) RETURN collect(n) as row",
                    (row) -> assertEquals(1800, ((List) row.get("row")).size()));
        }

        try (Session session = driver.session(SessionConfig.forDatabase(DB_BAR))) {
            session.writeTransaction(tx -> tx.run(
                    "UNWIND range(1,2000) as range CREATE (n:Foo {id: range}) WITH n CALL apoc.ttl.expireIn(n,500,'ms') RETURN count(*)")
            );

            TestContainerUtil.testCall(
                    session,
                    "MATCH (n:Foo) RETURN collect(n) as row",
                    (row) -> assertEquals(2000, ((List) row.get("row")).size()));

            Thread.sleep(2000);

            TestContainerUtil.testCall(
                    session,
                    "MATCH (n:Foo) RETURN collect(n) as row",
                    (row) -> assertEquals(1000, ((List) row.get("row")).size()));
        }
    }
}
