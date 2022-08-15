package apoc.generate.config;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class NumberOfNodesBasedConfigTest {
    @Test
    public void shouldCorrectlyEvaluateValidConfig() {
        assertFalse(new NumberOfNodesBasedConfig(-1).isValid());
        assertFalse(new NumberOfNodesBasedConfig(0).isValid());
        assertFalse(new NumberOfNodesBasedConfig(1).isValid());

        assertTrue(new NumberOfNodesBasedConfig(3).isValid());
        assertTrue(new NumberOfNodesBasedConfig(5).isValid());

        assertTrue(new NumberOfNodesBasedConfig(Integer.MAX_VALUE - 1).isValid());
        assertTrue(new NumberOfNodesBasedConfig(Integer.MAX_VALUE).isValid());
        //noinspection NumericOverflow
        assertFalse(new NumberOfNodesBasedConfig(Integer.MAX_VALUE + 1).isValid());
    }
}
