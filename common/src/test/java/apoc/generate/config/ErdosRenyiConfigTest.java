package apoc.generate.config;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Unit test for {@link ErdosRenyiConfig}.
 */
public class ErdosRenyiConfigTest {

    @Test
    public void shouldCorrectlyEvaluateValidConfig() {
        assertFalse(new ErdosRenyiConfig(-1, 5).isValid());
        assertFalse(new ErdosRenyiConfig(0, 5).isValid());
        assertFalse(new ErdosRenyiConfig(1, 5).isValid());
        assertFalse(new ErdosRenyiConfig(2, 5).isValid());
        assertFalse(new ErdosRenyiConfig(3, 5).isValid());
        assertTrue(new ErdosRenyiConfig(4, 5).isValid());
        assertTrue(new ErdosRenyiConfig(5, 5).isValid());
        assertTrue(new ErdosRenyiConfig(10000, 5).isValid());

        assertFalse(new ErdosRenyiConfig(10000, -1).isValid());
        assertFalse(new ErdosRenyiConfig(10000, 0).isValid());
        assertTrue(new ErdosRenyiConfig(10000, 1).isValid());

        assertTrue(new ErdosRenyiConfig(10_000, 5_000 * (10_000 - 1)).isValid());
        assertFalse(new ErdosRenyiConfig(10_000, 5_000 * (10_000 - 1) + 1).isValid());
        assertFalse(new ErdosRenyiConfig(50_000, 25_000 * (50_000 - 1) + 1).isValid());

        assertTrue(new ErdosRenyiConfig(10_000_000, 500_000_000).isValid());
        assertTrue(new ErdosRenyiConfig(100_000_000, 500_000_000).isValid());
        assertTrue(new ErdosRenyiConfig(1_000_000_000, 2_000_000_000).isValid());
        assertTrue(new ErdosRenyiConfig(50_000, 1_249_974_999).isValid());
        assertTrue(new ErdosRenyiConfig(50_000, 1_249_975_000).isValid());
        assertFalse(new ErdosRenyiConfig(50_000, 1_249_975_001).isValid());
        assertFalse(new ErdosRenyiConfig(50_000, 1_249_975_002).isValid());
    }
}
