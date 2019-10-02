package apoc.generate;

import apoc.generate.config.BarabasiAlbertConfig;
import apoc.generate.config.BasicGeneratorConfig;
import apoc.generate.node.SocialNetworkNodeCreator;
import apoc.generate.relationship.BarabasiAlbertRelationshipGenerator;
import apoc.generate.relationship.SocialNetworkRelationshipCreator;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;

/**
 * Integration test for {@link Neo4jGraphGenerator} with {@link BarabasiAlbertRelationshipGenerator}.
 */
@RunWith(Parameterized.class)
public class BarabasiAlbertGeneratorParameterizedTest {

    @Parameterized.Parameters
    public static Collection<Integer[]> data() {
        return Arrays.asList(new Integer[][]{
                {100,2},
                {100,3},
                {100,4},
                {100,5},
                {10,7},
                {1000,2}
        });
    }

    @Parameterized.Parameter(0)
    public int numberOfNodes;

    @Parameterized.Parameter(1)
    public int numberOfEdges;

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();


    @Test
    public void shouldGenerateCorrectNumberOfNodesAndRelationships() throws Exception {
        new Neo4jGraphGenerator(db).generateGraph(new BasicGeneratorConfig(
                new BarabasiAlbertRelationshipGenerator(new BarabasiAlbertConfig(numberOfNodes, numberOfEdges)),
                new SocialNetworkNodeCreator(),
                new SocialNetworkRelationshipCreator()
        ));
        try (Transaction tx = db.beginTx()) {
            assertEquals(numberOfNodes, Iterables.count(tx.getAllNodes()));
            assertEquals(numberOfNodes * numberOfEdges - (numberOfEdges * (numberOfEdges + 1) / 2), Iterables.count( tx.getAllRelationships()));
            tx.commit();
        }
    }
}
