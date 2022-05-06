package apoc.cypher;

import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CypherIsVersionDifferentTest {

    @Test
    public void shouldReturnFalseOnlyWithCompatibleVersion() {
        assertTrue(CypherInitializer.isVersionDifferent(List.of("3.5"), "4.4.0.2"));
        assertTrue(CypherInitializer.isVersionDifferent(List.of("5_0"), "4.4.0.2"));

        // we expect that APOC versioning is always consistent to Neo4j versioning
        assertFalse(CypherInitializer.isVersionDifferent(List.of("5_0"), "5_0_0_1"));
    }
}
