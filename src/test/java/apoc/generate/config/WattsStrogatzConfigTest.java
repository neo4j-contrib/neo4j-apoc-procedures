package apoc.generate.config;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Unit test for {@link WattsStrogatzConfig}.
 */
public class WattsStrogatzConfigTest {

    @Test
    public void shouldCorrectlyEvaluateValidConfig() {
        assertFalse(new WattsStrogatzConfig(-1, 4, 0.5).isValid());
        assertFalse(new WattsStrogatzConfig(0, 4, 0.5).isValid());
        assertFalse(new WattsStrogatzConfig(1, 4, 0.5).isValid());
        assertFalse(new WattsStrogatzConfig(2, 4, 0.5).isValid());
        assertFalse(new WattsStrogatzConfig(3, 4, 0.5).isValid());
        assertFalse(new WattsStrogatzConfig(4, 4, 0.5).isValid());
        assertTrue(new WattsStrogatzConfig(5, 4, 0.5).isValid());
        assertTrue(new WattsStrogatzConfig(6, 4, 0.5).isValid());

        assertFalse(new WattsStrogatzConfig(6, 3, 0.5).isValid());
        assertFalse(new WattsStrogatzConfig(6, 2, 0.5).isValid());
        assertFalse(new WattsStrogatzConfig(6, 1, 0.5).isValid());
        assertFalse(new WattsStrogatzConfig(6, 0, 0.5).isValid());
        assertFalse(new WattsStrogatzConfig(6, -1, 0.5).isValid());
        assertFalse(new WattsStrogatzConfig(6, 5, 0.5).isValid());
        assertFalse(new WattsStrogatzConfig(6, 6, 0.5).isValid());
        assertFalse(new WattsStrogatzConfig(6, 7, 0.5).isValid());
        assertFalse(new WattsStrogatzConfig(6, 8, 0.5).isValid());
        assertFalse(new WattsStrogatzConfig(6, 111, 0.5).isValid());

        assertFalse(new WattsStrogatzConfig(6, 4, -0.01).isValid());
        assertTrue(new WattsStrogatzConfig(6, 4, 0).isValid());
        assertTrue(new WattsStrogatzConfig(6, 4, 0.01).isValid());
        assertTrue(new WattsStrogatzConfig(6, 4, 0.99).isValid());
        assertTrue(new WattsStrogatzConfig(6, 4, 1.00).isValid());
        assertFalse(new WattsStrogatzConfig(6, 4, 1.01).isValid());
    }
}
