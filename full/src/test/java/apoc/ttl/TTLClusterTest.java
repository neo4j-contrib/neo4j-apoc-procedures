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
    }

    @AfterClass
    public static void bringDownCluster() {
        if (cluster != null) {
            cluster.close();
        }
    }

    @Test
    public void testIfSpecificDbKeyTrueAndGeneralKeyFalse() throws Exception {

        cluster = TestContainerUtil.createEnterpriseCluster(
                                3,
                                0,
                                Collections.emptyMap(),
                                Map.of("apoc.ttl.enabled.neo4j", "true",
                                        "apoc.ttl.enabled", "false",
                                        "apoc.ttl.schedule", "2")
        );
        Assume.assumeNotNull(cluster);

        cluster.getSession().writeTransaction(tx -> tx.run(
                "UNWIND range(1,100) as range CREATE (n:Foo {id: range}) WITH n CALL apoc.ttl.expireIn(n,500,'ms') RETURN count(*)"
        ));

        Thread.sleep(4000);

        try(Session session = cluster.getDriver().session()) {
            TestContainerUtil.testCall(
                    session,
                    "MATCH (n:Foo) RETURN collect(n) as row",
                    (row) -> assertEquals(0, ((List)row.get("row")).size()));
        }

    }

    @Test
    public void testIfSpecificDbKeyFalseAndGeneralKeyTrue() throws Exception {
        cluster = TestContainerUtil.createEnterpriseCluster(
                3,
                0,
                Collections.emptyMap(),
                Map.of("apoc.ttl.enabled.neo4j", "false",
                        "apoc.ttl.enabled", "true",
                        "apoc.ttl.schedule", "2")
        );
        Assume.assumeNotNull(cluster);

        cluster.getSession().writeTransaction(tx -> tx.run(
                "UNWIND range(1,100) as range CREATE (n:Foo {id: range}) WITH n CALL apoc.ttl.expireIn(n,500,'ms') RETURN count(*)"
        ));

        Thread.sleep(4000);

        try(Session session = cluster.getDriver().session()) {
            TestContainerUtil.testCall(
                    session,
                    "MATCH (n:Foo) RETURN collect(n) as row",
                    (row) -> assertEquals(100, ((List)row.get("row")).size()));
        }
    }

    @Test
    public void testIfSpecificDbKeyFalseAndGeneralKeyFalse() throws Exception {
        cluster = TestContainerUtil.createEnterpriseCluster(
                3,
                0,
                Collections.emptyMap(),
                Map.of("apoc.ttl.enabled.neo4j", "false",
                        "apoc.ttl.enabled", "false",
                        "apoc.ttl.schedule", "2")
        );
        Assume.assumeNotNull(cluster);

        cluster.getSession().writeTransaction(tx -> tx.run(
                "UNWIND range(1,100) as range CREATE (n:Foo {id: range}) WITH n CALL apoc.ttl.expireIn(n,500,'ms') RETURN count(*)"
        ));

        Thread.sleep(4000);

        try(Session session = cluster.getDriver().session()) {
            TestContainerUtil.testCall(
                    session,
                    "MATCH (n:Foo) RETURN collect(n) as row",
                    (row) -> assertEquals(100, ((List)row.get("row")).size()));
        }
    }

    @Test
    public void testIfSpecificDbKeyTrueAndGeneralKeyTrue() throws Exception {
        cluster = TestContainerUtil.createEnterpriseCluster(
                3,
                0,
                Collections.emptyMap(),
                Map.of("apoc.ttl.enabled.neo4j", "true",
                        "apoc.ttl.enabled", "true",
                        "apoc.ttl.schedule", "2")
        );
        Assume.assumeNotNull(cluster);

        cluster.getSession().writeTransaction(tx -> tx.run(
                "UNWIND range(1,100) as range CREATE (n:Foo {id: range}) WITH n CALL apoc.ttl.expireIn(n,500,'ms') RETURN count(*)"
        ));

        Thread.sleep(4000);

        try(Session session = cluster.getDriver().session()) {
            TestContainerUtil.testCall(
                    session,
                    "MATCH (n:Foo) RETURN collect(n) as row",
                    (row) -> assertEquals(0, ((List)row.get("row")).size()));
        }
    }

    @Test
    public void testIfSpecificDbKeyNotPresent() throws Exception {
        cluster = TestContainerUtil.createEnterpriseCluster(
                3,
                0,
                Collections.emptyMap(),
                Map.of("apoc.ttl.enabled", "false",
                        "apoc.ttl.schedule", "2")
        );
        Assume.assumeNotNull(cluster);

        cluster.getSession().writeTransaction(tx -> tx.run(
                "UNWIND range(1,100) as range CREATE (n:Foo {id: range}) WITH n CALL apoc.ttl.expireIn(n,500,'ms') RETURN count(*)"
        ));

        Thread.sleep(4000);

        try(Session session = cluster.getDriver().session()) {
            TestContainerUtil.testCall(
                    session,
                    "MATCH (n:Foo) RETURN collect(n) as row",
                    (row) -> assertEquals(100, ((List)row.get("row")).size()));
        }
    }

    @Test
    public void testIfSpecificDbLeyNotExist() throws Exception {
        cluster = TestContainerUtil.createEnterpriseCluster(
                3,
                0,
                Collections.emptyMap(),
                Map.of("apoc.ttl.enabled.notexistent", "true",
                        "apoc.ttl.enabled", "false",
                        "apoc.ttl.schedule", "2")
        );
        Assume.assumeNotNull(cluster);

        cluster.getSession().writeTransaction(tx -> tx.run(
                "UNWIND range(1,100) as range CREATE (n:Foo {id: range}) WITH n CALL apoc.ttl.expireIn(n,500,'ms') RETURN count(*)"
        ));

        Thread.sleep(4000);

        try(Session session = cluster.getDriver().session()) {
            TestContainerUtil.testCall(
                    session,
                    "MATCH (n:Foo) RETURN collect(n) as row",
                    (row) -> assertEquals(100, ((List)row.get("row")).size()));
        }
    }
}
