package apoc.trigger;

import apoc.util.TestUtil;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.dbms.api.DatabaseNotFoundException;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.List;

import static apoc.ApocSettings.apoc_trigger_enabled;
import static apoc.util.TestUtil.testCall;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.configuration.GraphDatabaseSettings.procedure_unrestricted;

/**
 * Test class for non-deprecated procedures, 
 * i.e. `apoc.trigger.install`, `apoc.trigger.drop`, `apoc.trigger.dropAll`, `apoc.trigger.stop`, and `apoc.trigger.start`
 */
public class TriggerNewProceduresTest {

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule()
            .withSetting(procedure_unrestricted, List.of("apoc*"))
            .withSetting(apoc_trigger_enabled, true);  // need to use settings here, apocConfig().setProperty in `setUp` is too late

    @Before
    public void setUp() throws Exception {
        TestUtil.registerProcedure(db, Trigger.class);
    }

    @Test
    public void testInstallTriggerInWrongDb() {
        try {
            testCall(db, "CALL apoc.trigger.install('notExistent', 'name', 'RETURN 1',{})", 
                    r -> fail("Should fail because of database not found"));
        } catch (QueryExecutionException e) {
            assertTrue(e.getMessage().contains(DatabaseNotFoundException.class.getName()));
        }
    }

    @Test
    public void testInstallTriggerInSystemDb() {
        try {
            testCall(db, "CALL apoc.trigger.install('system', 'name', 'RETURN 1',{})", 
                    r -> fail("Should fail because of unrecognised system procedure"));
        } catch (QueryExecutionException e) {
            assertTrue(e.getMessage().contains("Not a recognised system command or procedure"));
        }
    }

}
