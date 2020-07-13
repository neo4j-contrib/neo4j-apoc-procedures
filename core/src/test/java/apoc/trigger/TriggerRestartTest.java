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
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

import java.util.Collections;
import java.util.Map;

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
        TestUtil.registerProcedure(db, Trigger.class);
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

//        db.execute("CALL apoc.trigger.add('myTrigger', 'unwind $createdNodes as n set n.trigger=true', {phase:'before'})");
        TestUtil.testResult(db, "CALL apoc.trigger.add('myTrigger', 'unwind $createdNodes as n set n.trigger=true', {phase:'before'})",
                result -> {
                    Map<String, Object> single = Iterators.single(result);
                    System.out.println(single);
                });
        db.executeTransactionally("CREATE (p:Person{id:1})");
        TestUtil.testCallCount(db, "match (n:Person{trigger:true}) return n", Collections.emptyMap(), 1);

        restartDb();

        db.executeTransactionally("CREATE (p:Person{id:2})");
        TestUtil.testCallCount(db, "match (n:Person{trigger:true}) return n", Collections.emptyMap(), 2);
    }
}
