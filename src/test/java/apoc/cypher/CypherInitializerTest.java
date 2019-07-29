package apoc.cypher;

import apoc.util.TestUtil;
import apoc.util.Utils;
import org.junit.After;
import org.junit.Test;
import org.neo4j.common.DependencyResolver;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.internal.helpers.Listeners;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.kernel.availability.AvailabilityGuard;
import org.neo4j.kernel.availability.AvailabilityListener;
import org.neo4j.kernel.availability.DatabaseAvailabilityGuard;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.ReflectionUtil;

import static apoc.ApocSettings.dynamic;
import static apoc.util.TestUtil.apocGraphDatabaseBuilder;
import static apoc.util.TestUtil.testResult;
import static org.junit.Assert.assertEquals;
import static org.neo4j.configuration.SettingValueParsers.STRING;

public class CypherInitializerTest {

    public GraphDatabaseService db;
    public DatabaseManagementService dbms;

    public void init(String... initializers) {

        Pair<DatabaseManagementService, GraphDatabaseService> pair = apocGraphDatabaseBuilder(builder -> {
            if (initializers.length == 1) {
            } else {
                int index = 1;
                for (String initializer: initializers) {
                    builder.setConfig(dynamic("apoc.initializer.cypher." + index++, STRING), initializer);
                }
            }
        });

        dbms = pair.first();
        db = pair.other();

        // NB we need to register at least one procedure with name "apoc", otherwise initializer will not get called
        TestUtil.registerProcedure(db, Utils.class);
        waitForInitializerBeingFinished();
    }

    private void waitForInitializerBeingFinished() {
        CypherInitializer initializer = getInitializer();
        while (!initializer.isFinished()) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * get a reference to CypherInitializer for diagnosis. This needs to use reflection.
     * @return
     */
    private CypherInitializer getInitializer() {
        GraphDatabaseAPI api = (GraphDatabaseAPI) db;
        DatabaseAvailabilityGuard availabilityGuard = (DatabaseAvailabilityGuard) api.getDependencyResolver().resolveDependency(AvailabilityGuard.class, DependencyResolver.SelectionStrategy.FIRST);
        try {
            Listeners<AvailabilityListener> listeners = ReflectionUtil.getPrivateField(availabilityGuard, "listeners", Listeners.class);

            for (AvailabilityListener listener: listeners) {
                if (listener instanceof CypherInitializer) {
                    return (CypherInitializer) listener;
                }
            }
            throw new IllegalStateException("found no cypher initializer");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    @After
    public void teardown() {
        dbms.shutdown();
    }

    @Test
    public void noInitializerWorks() {
        init();
        expectNodeCount(0);
    }

    @Test
    public void emptyInitializerWorks() {
        init("");
        expectNodeCount(0);
    }

    @Test
    public void singleInitializerWorks() {
        init("create()");
        expectNodeCount(1);
    }

    @Test
    public void multipleInitializersWorks() {
        init("create ()", "match (n) create ()");  // this only creates 2 nodes if the statements run in same order
        expectNodeCount(2);
    }

    @Test
    public void multipleInitializersWorks2() {
        init("match (n) create ()", "create ()");
        expectNodeCount(1);
    }

    private void expectNodeCount(int i) {
        testResult(db, "match (n) return n", result -> assertEquals(i, Iterators.count(result)));
    }

}
