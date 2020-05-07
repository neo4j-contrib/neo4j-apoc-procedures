package apoc.trigger;

import apoc.util.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.internal.kernel.api.exceptions.KernelException;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

public class TriggerRestartTest {

    @Rule
    public TemporaryFolder store_dir = new TemporaryFolder();

    private GraphDatabaseService db;

    @Before
    public void setUp() throws KernelException {
        db = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(store_dir.getRoot())
                .setConfig("apoc.trigger.enabled", "true")
                .newGraphDatabase();
        TestUtil.registerProcedure(db, Trigger.class);
    }

    @After
    public void tearDown() throws IOException {
        db.shutdown();
    }

    private void restartDb() throws KernelException {
        db.shutdown();
        setUp();
    }

    @Test
    public void testTriggerRunsAfterRestart() throws Exception {

//        db.execute("CALL apoc.trigger.add('myTrigger', 'unwind $createdNodes as n set n.trigger=true', {phase:'before'})");
        TestUtil.testResult(db, "CALL apoc.trigger.add('myTrigger', 'unwind $createdNodes as n set n.trigger=true', {phase:'before'})",
                result -> {
                    Map<String, Object> single = Iterators.single(result);
                    System.out.println(single);
                });
        db.execute("CREATE (p:Person{id:1})");
        TestUtil.testCallCount(db, "match (n:Person{trigger:true}) return n", Collections.emptyMap(), 1);

        restartDb();

        db.execute("CREATE (p:Person{id:2})");
        TestUtil.testCallCount(db, "match (n:Person{trigger:true}) return n", Collections.emptyMap(), 2);
    }
}
