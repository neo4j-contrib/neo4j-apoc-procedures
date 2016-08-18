package apoc.generate;

import apoc.generate.config.BasicGeneratorConfig;
import apoc.generate.config.ErdosRenyiConfig;
import apoc.generate.config.GeneratorConfiguration;
import apoc.generate.node.SocialNetworkNodeCreator;
import apoc.generate.relationship.ErdosRenyiRelationshipGenerator;
import apoc.generate.relationship.SocialNetworkRelationshipCreator;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.junit.Assert.assertEquals;
import static org.neo4j.helpers.collection.Iterables.count;

/**
 * Integration test for {@link Neo4jGraphGenerator} with
 * {@link ErdosRenyiRelationshipGenerator}.
 */
public class ErdosRenyiGeneratorTest {

    @Test
    public void shouldGenerateCorrectNumberOfNodesAndRelationships() throws Exception {
        assertUsingDatabase(100, 200);
        assertUsingDatabase(100, 300);
        assertUsingDatabase(100, 1000);
        assertUsingDatabase(100, 5);
        assertUsingDatabase(10, 11);
        assertUsingDatabase(10, 23);
        assertUsingDatabase(20, 190);
    }

    @Test(timeout = 60 * 1000)
    @Ignore
    public void shouldGenerateRelationshipsForLargeGraphInAReasonableAmountOfTime() {
        new ErdosRenyiRelationshipGenerator(new ErdosRenyiConfig(500_000, 10_000_000)).generateEdges();
    }

    @Test(timeout = 60 * 1000)
    @Ignore
    public void shouldGenerateRelationshipsForLargeGraphInAReasonableAmountOfTime2() {
        new ErdosRenyiRelationshipGenerator(new ErdosRenyiConfig(10000, 25_000_000)).generateEdges();
    }

    private void assertUsingDatabase(int numberOfNodes, int numberOfEdges) {
        GraphDatabaseService database = new TestGraphDatabaseFactory().newImpermanentDatabase();

        new Neo4jGraphGenerator(database).generateGraph(getGeneratorConfiguration(numberOfNodes, numberOfEdges));

        assertCorrectNumberOfNodesAndRelationships(database, numberOfNodes, numberOfEdges);

        database.shutdown();
    }

    private void assertCorrectNumberOfNodesAndRelationships(GraphDatabaseService database, int numberOfNodes, int numberOfEdges) {
        try (Transaction tx = database.beginTx()) {
            assertEquals(numberOfNodes, count(database.getAllNodes()));
            assertEquals(numberOfEdges, count(database.getAllRelationships()));

            tx.success();
        }
    }

    private GeneratorConfiguration getGeneratorConfiguration(int numberOfNodes, int numberOfEdges) {
        return new BasicGeneratorConfig(
                new ErdosRenyiRelationshipGenerator(new ErdosRenyiConfig(numberOfNodes, numberOfEdges)),
                new SocialNetworkNodeCreator(),
                new SocialNetworkRelationshipCreator()
        );
    }
}
