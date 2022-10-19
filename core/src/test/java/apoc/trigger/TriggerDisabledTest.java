package apoc.trigger;

import apoc.util.TestUtil;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.neo4j.graphdb.Result;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.Map;

import static apoc.ApocConfig.APOC_TRIGGER_ENABLED;
import static apoc.ApocConfig.apocConfig;

/**
 * @author alexiudice
 * @since 14.07.18
 * <p>
 * Tests for fix of #845.
 * <p>
 * Testing disabled triggers needs to be a different test file from 'TriggerTest.java' since
 * Trigger classes and methods are static and 'TriggerTest.java' instantiates a class that enables triggers.
 *
 * NOTE: this test class expects every method to fail with a RuntimeException
 */
public class TriggerDisabledTest {

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();

    @Rule
    public ExpectedException exceptionRule = ExpectedException.none();

    @Before
    public void setUp() throws Exception {
        apocConfig().setProperty(APOC_TRIGGER_ENABLED, false);
        exceptionRule.expect(RuntimeException.class);
        exceptionRule.expectMessage(TriggerUtils.NOT_ENABLED_ERROR);
        TestUtil.registerProcedure(db, Trigger.class, TriggerDeprecatedProcedures.class);
    }

    @Test
    public void testTriggerDisabledList() {
        db.executeTransactionally("CALL apoc.trigger.list() YIELD name RETURN name", Map.of(), Result::resultAsString);
    }

    @Test
    public void testTriggerDisabledAdd() {
        db.executeTransactionally("CALL apoc.trigger.add('test-trigger', 'RETURN 1', {phase: 'before'}) YIELD name RETURN name");
    }

    @Test
    public void testTriggerDisabledRemove() {
        db.executeTransactionally("CALL apoc.trigger.remove('test-trigger')");
    }

    @Test
    public void testTriggerDisabledResume() {
        db.executeTransactionally("CALL apoc.trigger.resume('test-trigger')");
    }

    @Test
    public void testTriggerDisabledPause() {
        db.executeTransactionally("CALL apoc.trigger.pause('test-trigger')");
    }

    @Test
    public void testTriggerDisabledInstall() {
        db.executeTransactionally("CALL apoc.trigger.install('neo4j', 'test-trigger', 'RETURN 1', {phase: 'before'})");
    }
    @Test
    public void testTriggerDisabledDrop() {
        db.executeTransactionally("CALL apoc.trigger.drop('neo4j', 'test-trigger')");
    }
    
    @Test
    public void testTriggerDisabledDropAll() {
        db.executeTransactionally("CALL apoc.trigger.dropAll('neo4j')");
    }

    @Test
    public void testTriggerDisabledStart() {
        db.executeTransactionally("CALL apoc.trigger.start('neo4j', 'test-trigger')");
    }

    @Test
    public void testTriggerDisabledStop() {
        db.executeTransactionally("CALL apoc.trigger.stop('neo4j', 'test-trigger')");
    }
}
