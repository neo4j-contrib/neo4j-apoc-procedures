package apoc.ttl;

import apoc.util.*;
import org.junit.*;
import org.neo4j.driver.Session;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import static apoc.util.TestUtil.isTravis;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeFalse;


public class TTLClusterTest {

    private static TestcontainersCausalCluster cluster;

    @BeforeClass
    public static void setupCluster() {
        assumeFalse(isTravis());
        TestUtil.ignoreException(() -> cluster = TestContainerUtil
                        .createEnterpriseCluster(3, 0,
                                Collections.emptyMap(),
                                Map.of("apoc.ttl.enabled.dbtest", "false",
                                        "apoc.ttl.enabled", "true",
                                        "apoc.ttl.schedule", "2")
                        ),
                Exception.class);
        Assume.assumeNotNull(cluster);
    }

    @After
    public void cleanDb() {
        try (Session session = cluster.getDriver().session()) {
            session.writeTransaction(tx -> tx.run("MATCH (n) DETACH DELETE n;"));
        }
    }

    @AfterClass
    public static void bringDownCluster() {
        if (cluster != null) {
            cluster.close();
        }
    }

    @Test
    public void testWithSpecificDatabaseCreated() throws Exception {

        try (Session session = cluster.getDriver().session()) {
            session.writeTransaction(tx -> tx.run("CREATE DATABASE dbtest;" ));
        }
        Thread.sleep(1000);

        cluster.getSession().writeTransaction(tx -> tx.run(
                "UNWIND range(1,100) as range CREATE (n:Foo {id: range}) WITH n CALL apoc.ttl.expireIn(n,500,'ms') RETURN count(*)"
        ));
        Thread.sleep(100);

        try (Session session = cluster.getDriver().session()) {
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
    public void testWithSpecificDatabaseNotCreated() throws Exception {

        cluster.getSession().writeTransaction(tx -> tx.run(
                "UNWIND range(1,100) as range CREATE (n:Foo {id: range}) WITH n CALL apoc.ttl.expireIn(n,500,'ms') RETURN count(*)"
        ));
        Thread.sleep(100);

        try (Session session = cluster.getDriver().session()) {
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
}
