package apoc.cypher;

import org.junit.Test;

import static apoc.cypher.CypherInitializer.isVersionDifferent;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CypherIsVersionDifferentTest {

    @Test
    public void shouldReturnFalseOnlyWithCompatibleVersion() {
        assertTrue(isVersionDifferent("3.5", "4.4.0.2"));
        assertTrue(isVersionDifferent("5_0", "4.4.0.2"));
        assertFalse(isVersionDifferent("3.5.12", "3.5.0.9"));
        assertFalse(isVersionDifferent("3.5.12", "3.5.1.9"));
        assertFalse(isVersionDifferent("4.4.5", "4.4.0.4"));
        assertFalse(isVersionDifferent("4.4-aura", "4.4.0.4"));
        assertTrue(isVersionDifferent("4.4-aura", "4.3.0.4"));

        // we expect that APOC versioning is always consistent to Neo4j versioning
        assertTrue(isVersionDifferent("", "5_2_0_1"));
        assertTrue(isVersionDifferent("5_1_0", "5_0_0_1"));
        assertTrue(isVersionDifferent("5_1_0", "5_2_0_1"));
        assertTrue(isVersionDifferent("5_22_1", "5_2_0_1"));
        assertTrue(isVersionDifferent("5_2_1", "5_22_0_1"));
        assertTrue(isVersionDifferent("55_2_1", "5_2_1"));
        assertTrue(isVersionDifferent("55_2_1", "5_2_1_0_1"));
        assertTrue(isVersionDifferent("51-1-9-9", "5-1-1-9"));

        assertFalse(isVersionDifferent("4_4_5", "4.4.0.4"));
        assertFalse(isVersionDifferent("5_1_0", "5-1-0-1"));
        assertFalse(isVersionDifferent("5_22_1", "5_22_0_1"));
        assertFalse(isVersionDifferent("5_0", "5_0_0_1"));
        assertFalse(isVersionDifferent("5_0_1", "5_0_0_1"));
        
        assertFalse(isVersionDifferent("5-1-9-9", "5-1-0-1"));
    }
}
