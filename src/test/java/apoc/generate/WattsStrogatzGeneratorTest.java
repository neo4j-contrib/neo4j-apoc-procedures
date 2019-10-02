package apoc.generate;

import apoc.generate.config.BasicGeneratorConfig;
import apoc.generate.config.WattsStrogatzConfig;
import apoc.generate.node.SocialNetworkNodeCreator;
import apoc.generate.relationship.SocialNetworkRelationshipCreator;
import apoc.generate.relationship.WattsStrogatzRelationshipGenerator;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.neo4j.internal.helpers.collection.Iterables.count;

/**
 * Integration test for {@link Neo4jGraphGenerator} with {@link WattsStrogatzRelationshipGenerator}.
 */
@RunWith(Parameterized.class)
public class WattsStrogatzGeneratorTest {

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {100, 4, 0.1},
                {100, 6, 0.85},
                {100, 8, 0.5},
                {100, 10, 0.5},
                {1000, 50, 0.5}
        });
    }

    @Parameterized.Parameter(0)
    public int numberOfNodes;

    @Parameterized.Parameter(1)
    public int meanDegree;

    @Parameterized.Parameter(2)
    public double beta;

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();

    @Test
    public void shouldGenerateCorrectNumberOfNodesAndRelationships() throws Exception {
        new Neo4jGraphGenerator(db).generateGraph(new BasicGeneratorConfig(
                new WattsStrogatzRelationshipGenerator(new WattsStrogatzConfig(numberOfNodes, meanDegree, beta)),
                new SocialNetworkNodeCreator(),
                new SocialNetworkRelationshipCreator()
        ));
        try (Transaction tx = db.beginTx()) {
            assertEquals(numberOfNodes, count(tx.getAllNodes()));
            assertEquals((meanDegree * numberOfNodes) / 2, count(tx.getAllRelationships()));

            tx.commit();
        }
    }

    @Test(timeout = 2 * 60 * 1000)
    @Ignore
    public void shouldGenerateRelationshipsForLargeGraphInAReasonableAmountOfTime() {
        new WattsStrogatzRelationshipGenerator(new WattsStrogatzConfig(1_000_000, 10, 0.5)).generateEdges();
    }

}
