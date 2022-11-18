package apoc.generate.relationship;

import apoc.generate.Generate;
import apoc.generate.config.ErdosRenyiConfig;
import apoc.util.TestUtil;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.jupiter.api.AfterAll;

import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.List;

import static org.junit.Assert.assertEquals;


public class ErdosRenyiGraphRelationshipGeneratorTest  {

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();

    @Before
    public void setup() {
        TestUtil.registerProcedure(db, Generate.class);
    }

    @AfterAll
    public void tearDown() {
        db.shutdown();
    }

    @Test
    public void testErdosRenyiGeneratorValidity() {
        doGenerateEdges(20, 190); // Uses simple generator
        doGenerateEdges(10, 15); // Uses index <-> edge mapping
    }

    private void doGenerateEdges(int numberOfNodes, int numberOfEdges) {
        ErdosRenyiConfig config = new ErdosRenyiConfig(numberOfNodes, numberOfEdges);
        ErdosRenyiRelationshipGenerator er = new ErdosRenyiRelationshipGenerator(config);
        List<Pair<Integer, Integer>> edges = er.doGenerateEdges(); // Integer may not be enough here!

        assertCorrectNumberOfEdgesGenerated(numberOfEdges, edges);
    }

    /**
     * Checks the length of edgeList and compares to the expected number of edges to be generated
     *
     * @param numberOfEdges number of edges in the graph
     * @param edges list of edges as SameTypePair<Integer>
     */
    private void assertCorrectNumberOfEdgesGenerated(long numberOfEdges, List<Pair<Integer, Integer>> edges) {
        assertEquals(numberOfEdges, edges.size());
    }

}
