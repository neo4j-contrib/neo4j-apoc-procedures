package apoc.cypher;

import apoc.util.Utils;
import org.junit.After;
import org.junit.Test;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.helpers.Listeners;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.availability.AvailabilityGuard;
import org.neo4j.kernel.availability.AvailabilityListener;
import org.neo4j.kernel.availability.DatabaseAvailabilityGuard;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.ReflectionUtil;
import org.neo4j.test.TestGraphDatabaseFactory;

import static apoc.util.TestUtil.testResult;
import static org.junit.Assert.assertEquals;

public class CypherInitializerTest {

    public GraphDatabaseAPI db ;

    public void init(String... initializers) {
        GraphDatabaseBuilder graphDatabaseBuilder = new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder();
        if (initializers.length == 1) {
            graphDatabaseBuilder.setConfig("apoc.initializer.cypher", initializers[0]);
        } else {
            int index = 1;
            for (String initializer: initializers) {
                graphDatabaseBuilder.setConfig("apoc.initializer.cypher." + index++, initializer);
            }
        }

        db = (GraphDatabaseAPI) graphDatabaseBuilder.newGraphDatabase();

        // NB we need to register at least one procedure with name "apoc", otherwise initializer will not get called
        try {
            Procedures procedures = db.getDependencyResolver().resolveDependency(Procedures.class, DependencyResolver.SelectionStrategy.FIRST);
            procedures.registerProcedure(Utils.class);
        } catch (KernelException e) {
            throw new RuntimeException(e);
        }

        waitForInitializerBeingFinished();
    }

    private void waitForInitializerBeingFinished() {
        CypherInitializer initializer = getInitializer(db);
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
     * @param db
     * @return
     */
    private CypherInitializer getInitializer(GraphDatabaseAPI db) {
        DatabaseAvailabilityGuard availabilityGuard = (DatabaseAvailabilityGuard) db.getDependencyResolver().resolveDependency(AvailabilityGuard.class, DependencyResolver.SelectionStrategy.FIRST);
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
        db.shutdown();
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
