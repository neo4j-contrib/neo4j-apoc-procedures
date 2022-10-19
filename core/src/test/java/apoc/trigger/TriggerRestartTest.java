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

import java.util.Collections;

import static org.junit.Assert.assertTrue;

public class TriggerRestartTest {

    @Rule
    public TemporaryFolder store_dir = new TemporaryFolder();

    private GraphDatabaseService db;
    private DatabaseManagementService databaseManagementService;

    @Before
    public void setUp() {
        databaseManagementService = new TestDatabaseManagementServiceBuilder(store_dir.getRoot().toPath()).build();
        db = databaseManagementService.database(GraphDatabaseSettings.DEFAULT_DATABASE_NAME);
        ApocConfig.apocConfig().setProperty("apoc.trigger.enabled", "true");
        TestUtil.registerProcedure(db, Trigger.class, TriggerDeprecatedProcedures.class);
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
        final String query = "CALL apoc.trigger.add('myTrigger', 'unwind $createdNodes as n set n.trigger=true', {phase:'before'})";
        testTriggerRestartCommon(query);
    }

    @Test
    public void testTriggerViaInstallRunsAfterRestart() {
        final String query = "CALL apoc.trigger.install('neo4j', 'myTrigger', 'unwind $createdNodes as n set n.trigger=true', {phase:'before'})";
        testTriggerRestartCommon(query);
    }

    private void testTriggerRestartCommon(String query) {
        TestUtil.testCall(db, query, row -> {});
        
        db.executeTransactionally("CREATE (p:Person{id:1})");
        TestUtil.testCallCount(db, "match (n:Person{trigger:true}) return n", Collections.emptyMap(), 1);

        restartDb();

        db.executeTransactionally("CREATE (p:Person{id:2})");
        TestUtil.testCallCount(db, "match (n:Person{trigger:true}) return n", Collections.emptyMap(), 2);
    }
}
