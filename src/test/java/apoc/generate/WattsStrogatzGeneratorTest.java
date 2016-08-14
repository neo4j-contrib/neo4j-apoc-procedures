package apoc.generate;

import apoc.generate.config.BasicGeneratorConfig;
import apoc.generate.config.GeneratorConfiguration;
import apoc.generate.config.WattsStrogatzConfig;
import apoc.generate.node.SocialNetworkNodeCreator;
import apoc.generate.relationship.SocialNetworkRelationshipCreator;
import apoc.generate.relationship.WattsStrogatzRelationshipGenerator;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.neo4j.helpers.collection.Iterables.count;

/**
 * Integration test for {@link Neo4jGraphGenerator} with {@link WattsStrogatzRelationshipGenerator}.
 */
public class WattsStrogatzGeneratorTest {

    @Test
    public void shouldGenerateCorrectNumberOfNodesAndRelationships() throws Exception {
        assertUsingDatabase(100, 4, 0.1);
        assertUsingDatabase(100, 6, 0.85);
        assertUsingDatabase(100, 8, 0.5);
        assertUsingDatabase(100, 10, 0.5);
        assertUsingDatabase(1000, 50, 0.5);
    }

    @Test(timeout = 2 * 60 * 1000)
    @Ignore
    public void shouldGenerateRelationshipsForLargeGraphInAReasonableAmountOfTime() {
        new WattsStrogatzRelationshipGenerator(new WattsStrogatzConfig(1_000_000, 10, 0.5)).generateEdges();
    }

    private void assertUsingDatabase(int numberOfNodes, int meanDegree, double beta) {
        GraphDatabaseService database = new TestGraphDatabaseFactory().newImpermanentDatabase();

        new Neo4jGraphGenerator(database).generateGraph(getGeneratorConfiguration(numberOfNodes, meanDegree, beta));

        assertCorrectNumberOfNodesAndRelationships(database, numberOfNodes, meanDegree);

        database.shutdown();
    }

    private void assertCorrectNumberOfNodesAndRelationships(GraphDatabaseService database, int numberOfNodes, int meanDegree) {
        try (Transaction tx = database.beginTx()) {
            assertEquals(numberOfNodes, count(database.getAllNodes()));
            assertEquals((meanDegree * numberOfNodes) / 2, count(database.getAllRelationships()));

            tx.success();
        }
    }

    private GeneratorConfiguration getGeneratorConfiguration(int numberOfNodes, int meanDegree, double beta) {
        return new BasicGeneratorConfig(
                new WattsStrogatzRelationshipGenerator(new WattsStrogatzConfig(numberOfNodes, meanDegree, beta)),
                new SocialNetworkNodeCreator(),
                new SocialNetworkRelationshipCreator()
        );
    }

}
