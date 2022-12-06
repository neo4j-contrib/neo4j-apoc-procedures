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
import java.util.Collections;
import java.util.Map;

import static apoc.ApocConfig.SUN_JAVA_COMMAND;
import static apoc.trigger.TriggerTestUtil.TRIGGER_DEFAULT_REFRESH;
import static apoc.trigger.TriggerTestUtil.awaitTriggerDiscovered;
import static org.junit.Assert.assertEquals;

public class TriggerRestartTest {

    @Rule
    public TemporaryFolder store_dir = new TemporaryFolder();

    private GraphDatabaseService db;
    private GraphDatabaseService sysDb;
    private DatabaseManagementService databaseManagementService;

    @Before
    public void setUp() throws IOException {
        final File conf = store_dir.newFile("apoc.conf");
        try (FileWriter writer = new FileWriter(conf)) {
            writer.write("apoc.trigger.refresh=" + TRIGGER_DEFAULT_REFRESH);
        }
        System.setProperty(SUN_JAVA_COMMAND, "config-dir=" + store_dir.getRoot().getAbsolutePath());
        
        databaseManagementService = new TestDatabaseManagementServiceBuilder(store_dir.getRoot().toPath()).build();
        db = databaseManagementService.database(GraphDatabaseSettings.DEFAULT_DATABASE_NAME);
        sysDb = databaseManagementService.database(GraphDatabaseSettings.SYSTEM_DATABASE_NAME);
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
        sysDb = databaseManagementService.database(GraphDatabaseSettings.SYSTEM_DATABASE_NAME);
    }

    @Test
    public void testTriggerRunsAfterRestart() throws Exception {
        final String query = "CALL apoc.trigger.add('myTrigger', 'unwind $createdNodes as n set n.trigger = n.trigger + 1', {phase:'before'})";
        testTriggerWorksBeforeAndAfterRestart(db, query, Collections.emptyMap(), () -> {});
    }

    @Test
    public void testTriggerViaInstallRunsAfterRestart() {
        final String name = "myTrigger";
        final String innerQuery = "unwind $createdNodes as n set n.trigger = n.trigger + 1";
        final Map<String, Object> params = Map.of("name", name, "query", innerQuery);
        final String triggerQuery = "CALL apoc.trigger.install('neo4j', 'myTrigger', 'unwind $createdNodes as n set n.trigger = n.trigger + 1', {phase:'before'})";
        testTriggerWorksBeforeAndAfterRestart(sysDb, triggerQuery, params, () -> awaitTriggerDiscovered(db, name, innerQuery));
    }

    @Test
    public void testTriggerViaBothAddAndInstall() {
        // executing both trigger add and install with the same name will not duplicate the eventListeners
        final String name = "myTrigger";
        final String innerQuery = "unwind $createdNodes as n set n.trigger = n.trigger + 1";
        
        final String triggerQuery = "CALL apoc.trigger.add($name, $query, {phase:'before'})";

        final Map<String, Object> params = Map.of("name", name, "query", innerQuery);
        
        final Runnable runnable = () -> {
            sysDb.executeTransactionally("CALL apoc.trigger.install('neo4j', $name, $query, {phase:'before'})",
                    params);
            awaitTriggerDiscovered(db, name, innerQuery);
        };
        testTriggerWorksBeforeAndAfterRestart(db, triggerQuery, params, runnable);
    }

    private void testTriggerWorksBeforeAndAfterRestart(GraphDatabaseService gbs, String query, Map<String, Object> params, Runnable runnable) {
        TestUtil.testCall(gbs, query, params, row -> {});
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
}
