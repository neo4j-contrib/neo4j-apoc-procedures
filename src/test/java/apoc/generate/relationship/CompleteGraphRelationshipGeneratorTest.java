package apoc.generate.relationship;

import apoc.generate.config.NumberOfNodesBasedConfig;
import org.junit.Test;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.logging.Log;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CompleteGraphRelationshipGeneratorTest {

    @Test
    public void testCompleteGraphGenerator() {
        int numberOfNodes = 6;

        NumberOfNodesBasedConfig num = new NumberOfNodesBasedConfig(numberOfNodes);
        CompleteGraphRelationshipGenerator cg = new CompleteGraphRelationshipGenerator(num);

        List<Pair<Integer, Integer>> edges = cg.doGenerateEdges();

        assertIsComplete(edges, numberOfNodes);
    }

    private void assertIsComplete(List<Pair<Integer, Integer>> edges, int numberOfNodes) {
        assertEquals(edges.size(), (int) (.5 * numberOfNodes * (numberOfNodes - 1)));

        for (Integer i = 0; i < numberOfNodes; i++) {
            for (Integer j = i + 1; j < numberOfNodes; j++) {
                assertTrue(edges.contains(Pair.of(i, j)));
            }
        }
    }
}
