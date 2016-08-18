package apoc.generate.relationship;

import apoc.generate.config.WattsStrogatzConfig;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class WattsStrogatzRelationshipGeneratorTest {

    @Test
    public void testDoGenerateEdgesValidity() throws Exception {
        int meanDegree = 4;
        int numberOfNodes = 10;
        double betaCoefficient = 0.5;

        WattsStrogatzRelationshipGenerator generator = new WattsStrogatzRelationshipGenerator(new WattsStrogatzConfig(numberOfNodes, meanDegree, betaCoefficient));

        assertEquals((int) (meanDegree * numberOfNodes * .5), generator.doGenerateEdges().size());
    }

    @Test
    public void testDoGenerateEdgesPerformance() throws Exception {
        int meanDegree = 4;
        int numberOfNodes = 2_000;
        double betaCoefficient = 0.5;

        WattsStrogatzRelationshipGenerator generator = new WattsStrogatzRelationshipGenerator(new WattsStrogatzConfig(numberOfNodes, meanDegree, betaCoefficient));
        assertEquals((int) (meanDegree * numberOfNodes * .5), generator.doGenerateEdges().size());
    }
}