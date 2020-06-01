package apoc.generate;

import apoc.generate.config.BasicGeneratorConfig;
import apoc.generate.config.ErdosRenyiConfig;
import apoc.generate.node.SocialNetworkNodeCreator;
import apoc.generate.relationship.ErdosRenyiRelationshipGenerator;
import apoc.generate.relationship.SocialNetworkRelationshipCreator;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.runners.Parameterized.Parameters;

/**
 * Integration test for {@link Neo4jGraphGenerator} with
 * {@link ErdosRenyiRelationshipGenerator}.
 */
@RunWith(Parameterized.class)
public class ErdosRenyiGeneratorTest {

    @Parameters
    public static Collection<Integer[]> data() {
        return Arrays.asList(new Integer[][]{
                {100,200},
                {100,300},
                {100,1000},
                {100,5},
                {10,11},
                {10,23},
                {200,190},
        });
    }

    @Parameter(0)
    public int numberOfNodes;

    @Parameter(1)
    public int numberOfEdges;

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();

    @Test
    public void shouldGenerateCorrectNumberOfNodesAndRelationships() throws Exception {
        new Neo4jGraphGenerator(db).generateGraph(
                new BasicGeneratorConfig(
                    new ErdosRenyiRelationshipGenerator(new ErdosRenyiConfig(numberOfNodes, numberOfEdges)),
                new SocialNetworkNodeCreator(),
                new SocialNetworkRelationshipCreator()
        ));

        try (Transaction tx = db.beginTx()) {
            assertEquals(numberOfNodes, Iterables.count( tx.getAllNodes()));
            assertEquals(numberOfEdges, Iterables.count( tx.getAllRelationships()));
            tx.commit();
        }
    }

}
