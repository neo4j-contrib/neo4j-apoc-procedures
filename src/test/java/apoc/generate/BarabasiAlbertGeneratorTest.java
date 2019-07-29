package apoc.generate;

import apoc.generate.config.BarabasiAlbertConfig;
import apoc.generate.config.BasicGeneratorConfig;
import apoc.generate.config.GeneratorConfiguration;
import apoc.generate.node.SocialNetworkNodeCreator;
import apoc.generate.relationship.BarabasiAlbertRelationshipGenerator;
import apoc.generate.relationship.SocialNetworkRelationshipCreator;
import apoc.util.TestUtil;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.helpers.collection.Pair;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Integration test for {@link Neo4jGraphGenerator} with {@link BarabasiAlbertRelationshipGenerator}.
 */
public class BarabasiAlbertGeneratorTest {

    @Test
    public void shouldGenerateCorrectNumberOfNodesAndRelationships() throws Exception {
        assertUsingDatabase(100, 2);
        assertUsingDatabase(100, 3);
        assertUsingDatabase(100, 4);
        assertUsingDatabase(100, 5);
        assertUsingDatabase(10, 7);
        assertUsingDatabase(1000, 2);
    }

    @Test
    public void shouldGeneratePowerLawDistribution() {
        Pair<DatabaseManagementService, GraphDatabaseService> pair = TestUtil.apocGraphDatabaseBuilder();
        DatabaseManagementService dbms = pair.first();
        GraphDatabaseService database = pair.other();

        try {
            new Neo4jGraphGenerator(database).generateGraph(getGeneratorConfiguration(100, 2));

            List<Integer> degrees = new LinkedList<>();

            try (Transaction tx = database.beginTx()) {
                for (Node node : database.getAllNodes()) {
                    degrees.add(node.getDegree());
                }
                tx.success();
            }

            Collections.sort(degrees, Collections.reverseOrder());

            //todo make this an automated test
            //System.out.println(ArrayUtils.toString(degrees.toArray(new Integer[degrees.size()])));
        } finally {
            dbms.shutdown();
        }
    }

    @Test(timeout = 10 * 1000)
    @Ignore
    public void shouldGenerateRelationshipsForLargeGraphInAReasonableAmountOfTime() {
        new BarabasiAlbertRelationshipGenerator(new BarabasiAlbertConfig(1_000_000, 3)).generateEdges();
    }

    private void assertUsingDatabase(int numberOfNodes, int edgesPerNewNode) {
        Pair<DatabaseManagementService, GraphDatabaseService> pair = TestUtil.apocGraphDatabaseBuilder();
        DatabaseManagementService dbms = pair.first();
        GraphDatabaseService database = pair.other();

        new Neo4jGraphGenerator(database).generateGraph(getGeneratorConfiguration(numberOfNodes, edgesPerNewNode));

        assertCorrectNumberOfNodesAndRelationships(database, numberOfNodes, edgesPerNewNode);

        dbms.shutdown();
    }

    private void assertCorrectNumberOfNodesAndRelationships(GraphDatabaseService database, int numberOfNodes, int edgesPerNewNode) {
        try (Transaction tx = database.beginTx()) {
            assertEquals(numberOfNodes, Iterables.count(database.getAllNodes()));
            assertEquals(numberOfNodes * edgesPerNewNode - (edgesPerNewNode * (edgesPerNewNode + 1) / 2), Iterables.count(database.getAllRelationships()));

            tx.success();
        }
    }

    private GeneratorConfiguration getGeneratorConfiguration(int numberOfNodes, int edgesPerNewNode) {
        return new BasicGeneratorConfig(
                new BarabasiAlbertRelationshipGenerator(new BarabasiAlbertConfig(numberOfNodes, edgesPerNewNode)),
                new SocialNetworkNodeCreator(),
                new SocialNetworkRelationshipCreator()
        );
    }

}
