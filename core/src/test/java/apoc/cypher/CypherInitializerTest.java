package apoc.cypher;

import apoc.test.EnvSettingRule;
import apoc.test.annotations.Env;
import apoc.test.annotations.EnvSetting;
import apoc.util.TestUtil;
import apoc.util.Utils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.internal.helpers.Listeners;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.kernel.availability.AvailabilityGuard;
import org.neo4j.kernel.availability.AvailabilityListener;
import org.neo4j.kernel.availability.DatabaseAvailabilityGuard;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.ReflectionUtil;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.Collections;

import static apoc.ApocConfig.APOC_CONFIG_INITIALIZER;
import static apoc.ApocConfig.APOC_CONFIG_INITIALIZER_CYPHER;
import static apoc.util.CypherInitializerUtil.getInitializer;
import static org.junit.Assert.assertEquals;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;

public class CypherInitializerTest {

    public DbmsRule dbmsRule = new ImpermanentDbmsRule();

    @Rule
    public RuleChain ruleChain = new EnvSettingRule().around(dbmsRule);

    @Before
    public void waitForInitializerBeingFinished() {
        // we need at least on APOC proc being registered in finished CypherInitializers
        TestUtil.registerProcedure(dbmsRule, Utils.class);

        waitForInitializerBeingFinished(SYSTEM_DATABASE_NAME, dbmsRule);
        waitForInitializerBeingFinished(DEFAULT_DATABASE_NAME, dbmsRule);
    }

    private static void waitForInitializerBeingFinished(String dbName, DbmsRule dbmsRule) {
        CypherInitializer initializer = getInitializer(dbName, dbmsRule, CypherInitializer.class);
        while (!initializer.isFinished()) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
    }


    @Test
    @Env
    public void noInitializerWorks() {
        expectNodeCount(0);
    }

    @Test
    @Env({
            @EnvSetting(key= APOC_CONFIG_INITIALIZER_CYPHER, value="")
    })
    public void emptyInitializerWorks() {
        expectNodeCount(0);
    }

    @Test
    @Env({
            @EnvSetting(key= APOC_CONFIG_INITIALIZER_CYPHER, value="create()")
    })
    public void singleInitializerWorks() {
        expectNodeCount(1);
    }

    @Test
    @Env({  // this only creates 2 nodes if the statements run in same order
            @EnvSetting(key= APOC_CONFIG_INITIALIZER_CYPHER + ".0", value="create()"),
            @EnvSetting(key= APOC_CONFIG_INITIALIZER_CYPHER + ".1", value="match (n) create ()")
    })
    public void multipleInitializersWorks() {
        expectNodeCount(2);
    }

    @Test
    @Env({  // this only creates 1 node since the first statement doesn't do anything
            @EnvSetting(key= APOC_CONFIG_INITIALIZER_CYPHER + ".0", value="match (n) create ()"),
            @EnvSetting(key= APOC_CONFIG_INITIALIZER_CYPHER + ".1", value="create()")
    })
    public void multipleInitializersWorks2() {
        expectNodeCount(1);
    }

    @Test
    @Env({  // this only creates 2 nodes if the statements run in same order
            @EnvSetting(key= APOC_CONFIG_INITIALIZER + "." + SYSTEM_DATABASE_NAME, value="create user dummy set password 'abc'")
    })
    public void databaseSpecificInitializersForSystem() {
        GraphDatabaseService systemDb = dbmsRule.getManagementService().database(SYSTEM_DATABASE_NAME);
        long numberOfUsers = systemDb.executeTransactionally("show users", Collections.emptyMap(), Iterators::count);
        assertEquals(2l, numberOfUsers);
    }

    private void expectNodeCount(long i) {
        assertEquals(i, TestUtil.count(dbmsRule, "match (n) return n"));
    }
}
