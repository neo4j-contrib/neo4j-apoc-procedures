package apoc.trigger;

import apoc.ApocConfig;
import apoc.util.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import static apoc.ApocConfig.SUN_JAVA_COMMAND;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TriggerRestartTest {

    @Rule
    public TemporaryFolder store_dir = new TemporaryFolder();

    private GraphDatabaseService db;
    private DatabaseManagementService databaseManagementService;

    @Before
    public void setUp() throws IOException {
        final File conf = store_dir.newFile("apoc.conf");
        try (FileWriter writer = new FileWriter(conf)) {
            writer.write("apoc.trigger.refresh=100");
        }
        System.setProperty(SUN_JAVA_COMMAND, "config-dir=" + store_dir.getRoot().getAbsolutePath());
        
        databaseManagementService = new TestDatabaseManagementServiceBuilder(store_dir.getRoot().toPath()).build();
        db = databaseManagementService.database(GraphDatabaseSettings.DEFAULT_DATABASE_NAME);
        ApocConfig.apocConfig().setProperty("apoc.trigger.enabled", "true");
        TestUtil.registerProcedure(db, TriggerNewProcedures.class, Trigger.class);
    }

    @After
    public void tearDown() {
        databaseManagementService.shutdown();
    }

    private void restartDb() {
        databaseManagementService.shutdown();
        databaseManagementService = new TestDatabaseManagementServiceBuilder(store_dir.getRoot().toPath()).build();
        db = databaseManagementService.database(GraphDatabaseSettings.DEFAULT_DATABASE_NAME);
        assertTrue(db.isAvailable(1000));
    }

    @Test
    public void testTriggerRunsAfterRestart() throws Exception {
        final String query = "CALL apoc.trigger.add('myTrigger', 'unwind $createdNodes as n set n.trigger = n.trigger + 1', {phase:'before'})";
        testTriggerRestartCommon(query, () -> {});
    }

    @Test
    public void testTriggerViaInstallRunsAfterRestart() {
        final String query = "CALL apoc.trigger.install('neo4j', 'myTrigger', 'unwind $createdNodes as n set n.trigger = n.trigger + 1', {phase:'before'})";
        testTriggerRestartCommon(query, this::awaitTriggers);
    }

    @Test
    public void testTriggerViaBothAddAndInstall() {
        // executing both trigger add and install with the same name will not duplicate the eventListeners
        final String query = "CALL apoc.trigger.install('neo4j', 'myTrigger', 'unwind $createdNodes as n set n.trigger = n.trigger + 1', {phase:'before'})";
        final Runnable runnable = () -> {
            db.executeTransactionally("CALL apoc.trigger.install('neo4j', 'myTrigger', 'unwind $createdNodes as n set n.trigger = n.trigger + 1', {phase:'before'})");
            awaitTriggers();
        };
        testTriggerRestartCommon(query, runnable);
    }

    private void testTriggerRestartCommon(String query, Runnable runnable) {
        TestUtil.testCall(db, query, row -> {});
        runnable.run();
        
        db.executeTransactionally("CREATE (p:Person{id:1, trigger: 0})");
        TestUtil.testCall(db, "match (n:Person{id:1}) return n.trigger as trigger", 
                r -> assertEquals(1L, r.get("trigger")));

        restartDb();

        db.executeTransactionally("CREATE (p:Person{id:2, trigger: 0})");
        TestUtil.testCall(db, "match (n:Person{id:1}) return n.trigger as trigger",
                r -> assertEquals(1L, r.get("trigger")));
        TestUtil.testCall(db, "match (n:Person{id:2}) return n.trigger as trigger",
                r -> assertEquals(1L, r.get("trigger")));
    }

    private void awaitTriggers() {
        try {
            Thread.sleep(300);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
