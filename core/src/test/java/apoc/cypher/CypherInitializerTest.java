package apoc.cypher;

import apoc.ApocConfig;
import apoc.util.TestUtil;
import apoc.util.Utils;
import org.apache.commons.configuration2.Configuration;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.internal.helpers.Listeners;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.kernel.availability.AvailabilityGuard;
import org.neo4j.kernel.availability.AvailabilityListener;
import org.neo4j.kernel.availability.DatabaseAvailabilityGuard;
import org.neo4j.test.ReflectionUtil;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import static apoc.util.TestUtil.testResult;
import static org.junit.Assert.assertEquals;

public class CypherInitializerTest {

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();

    public void init(String... initializers) {

        Configuration config = db.resolveDependency(ApocConfig.class).getConfig();
        Iterators.stream(config.getKeys(ApocConfig.APOC_CONFIG_INITIALIZER_CYPHER)).forEach(k -> config.clearProperty(k));

        if (initializers.length == 1) {
            config.setProperty(ApocConfig.APOC_CONFIG_INITIALIZER_CYPHER, initializers[0]);
        } else {
            int index = 1;
            for (String initializer : initializers) {
                config.setProperty(ApocConfig.APOC_CONFIG_INITIALIZER_CYPHER + "." + index++, initializer);
            }
        }

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
        DatabaseAvailabilityGuard availabilityGuard = (DatabaseAvailabilityGuard) db.getDependencyResolver().resolveDependency(AvailabilityGuard.class);
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
