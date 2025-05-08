package apoc.ttl;

import apoc.periodic.Periodic;
import apoc.util.DbmsTestUtil;
import apoc.util.TestUtil;
import apoc.util.collection.Iterators;

import java.util.Map;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import org.junit.rules.TemporaryFolder;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;

import static apoc.ExtendedApocConfig.APOC_TTL_ENABLED;
import static apoc.ExtendedApocConfig.APOC_TTL_SCHEDULE;
import static apoc.util.TestUtil.waitDbsAvailable;
import static org.junit.Assert.assertTrue;

public class TTLTest {

    @ClassRule
    public static TemporaryFolder storeDir = new TemporaryFolder();

    private static GraphDatabaseService db;
    private static DatabaseManagementService databaseManagementService;

    @BeforeClass
    public static void beforeClass() throws Exception {
        databaseManagementService = DbmsTestUtil.startDbWithApocConfigs(storeDir,
                Map.of(APOC_TTL_ENABLED, "true",
                        APOC_TTL_SCHEDULE, "3")
        );

        db = databaseManagementService.database(GraphDatabaseSettings.DEFAULT_DATABASE_NAME);
        waitDbsAvailable(db);
        TestUtil.registerProcedure(db, TTL.class, Periodic.class);
    }

    @AfterClass
    public static void afterClass() {
        databaseManagementService.shutdown();
    }


    @Test
    public void testExpireManyNodes() {
        int fooCount = 200;
        int barCount = 300;
        db.executeTransactionally("UNWIND range(1," + fooCount + ") as range CREATE (:Baz)-[:REL_TEST]->(n:Foo:TTL {id: range, ttl: timestamp() + 100});");
        db.executeTransactionally("UNWIND range(1," + barCount + ") as range CREATE (n:Bar:TTL {id: range, ttl: timestamp() + 100});");
        assertTrue(isNodeCountConsistent(fooCount, barCount));
        org.neo4j.test.assertion.Assert.assertEventually(() -> isNodeCountConsistent(0, 0), (value) -> value, 30L, TimeUnit.SECONDS);
    }

    // test extracted from apoc.date
    @Test
    public void testExpire() {
        db.executeTransactionally("CREATE (n:Foo:TTL) SET n.ttl = timestamp() + 100");
        db.executeTransactionally("CREATE (n:Bar) WITH n CALL apoc.ttl.expireIn(n,500,'ms') RETURN count(*)");
        assertTrue(isNodeCountConsistent(1,1));
        org.neo4j.test.assertion.Assert.assertEventually(() -> isNodeCountConsistent(0, 0), (value) -> value, 10L, TimeUnit.SECONDS);
    }

    private static boolean isNodeCountConsistent(int foo, int bar) {
        try (Transaction tx = db.beginTx()) {
            boolean isNotCountConsistent = foo == Iterators.count(tx.findNodes(Label.label("Foo")))
                    && bar == Iterators.count(tx.findNodes(Label.label("Bar")))
                    && foo + bar == Iterators.count(tx.findNodes(Label.label("TTL")));
            tx.commit();
            return isNotCountConsistent;
        }
    }
}
