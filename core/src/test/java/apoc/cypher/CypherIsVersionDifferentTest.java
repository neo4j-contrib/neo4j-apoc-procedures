package apoc.cypher;

import org.junit.Test;

import java.util.List;

import static apoc.cypher.CypherInitializer.isVersionDifferent;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CypherIsVersionDifferentTest {

    @Test
    public void shouldReturnFalseOnlyWithCompatibleVersion() {
        assertTrue(isVersionDifferent(List.of("3.5"), "4.4.0.2", "."));
        assertTrue(isVersionDifferent(List.of("5_0"), "4.4.0.2", "."));
        assertFalse(isVersionDifferent(List.of("3.5.12"), "3.5.0.9", "."));
        assertFalse(isVersionDifferent(List.of("3.5.12"), "3.5.1.9", "."));
        assertFalse(isVersionDifferent(List.of("4.4.5"), "4.4.0.4", "."));

        // we expect that APOC versioning is always consistent to Neo4j versioning
        assertTrue(isVersionDifferent(List.of(""), "5_2_0_1", "_"));
        assertTrue(isVersionDifferent(List.of("5_1_0"), "5_0_0_1", "_"));
        assertTrue(isVersionDifferent(List.of("5_1_0"), "5-1,0,1", "_"));
        assertTrue(isVersionDifferent(List.of("5_1_0"), "5_2_0_1", "_"));
        assertTrue(isVersionDifferent(List.of("5_22_1"), "5_2_0_1", "_"));
        assertTrue(isVersionDifferent(List.of("5_2_1"), "5_22_0_1", "_"));
        assertTrue(isVersionDifferent(List.of("55_2_1"), "5_2_1", "_"));
        assertTrue(isVersionDifferent(List.of("55_2_1"), "5_2_1_0_1", "_"));
        assertTrue(isVersionDifferent(List.of("5-1,9,9"), "5-1,0,1", "_"));
        
        assertFalse(isVersionDifferent(List.of("5_22_1"), "5_22_0_1", "_"));
        assertFalse(isVersionDifferent(List.of("5_0"), "5_0_0_1", "_"));
        assertFalse(isVersionDifferent(List.of("5_0_1"), "5_0_0_1", "_"));
        
        assertFalse(isVersionDifferent(List.of("5-1-9-9"), "5-1-0-1", "-"));
    }
}
