package apoc.generate;

import apoc.generate.config.BasicGeneratorConfig;
import apoc.generate.config.DistributionBasedConfig;
import apoc.generate.config.GeneratorConfiguration;
import apoc.generate.config.InvalidConfigException;
import apoc.generate.node.NodeCreator;
import apoc.generate.node.SocialNetworkNodeCreator;
import apoc.generate.relationship.RelationshipCreator;
import apoc.generate.relationship.SimpleGraphRelationshipGenerator;
import apoc.generate.relationship.SocialNetworkRelationshipCreator;
import apoc.get.Get;
import apoc.util.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.neo4j.helpers.collection.Iterables.count;

/**
 * Smoke test for {@link Neo4jGraphGenerator}.
 */
public class Neo4jGraphGeneratorTest {

    private GraphDatabaseService db;

    @Before
    public void setUp() throws Exception {
        db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        TestUtil.registerProcedure(db, Generate.class);
    }

    @After
    public void tearDown() {
        db.shutdown();
    }

    @Test
    public void validDistributionShouldGenerateGraph() {
        NodeCreator nodeCreator = new SocialNetworkNodeCreator();
        RelationshipCreator relationshipCreator = new SocialNetworkRelationshipCreator();
        DistributionBasedConfig distribution = new DistributionBasedConfig(Arrays.asList(2, 2, 2, 2));
        SimpleGraphRelationshipGenerator relationshipGenerator = new SimpleGraphRelationshipGenerator(distribution);

        GeneratorConfiguration config = new BasicGeneratorConfig(relationshipGenerator, nodeCreator, relationshipCreator);

        new Neo4jGraphGenerator(db).generateGraph(config);

        try (Transaction tx = db.beginTx()) {
            assertEquals(4, count(db.getAllNodes()));
            assertEquals(4, count(db.getAllRelationships()));

            for (Node node : db.getAllNodes()) {
                assertEquals(2, node.getDegree());
            }

            tx.success();
        }
    }

    @Test(expected = InvalidConfigException.class)
    public void invalidDistributionShouldThrowException() {
        NodeCreator nodeCreator = new SocialNetworkNodeCreator();
        RelationshipCreator relationshipCreator = new SocialNetworkRelationshipCreator();
        DistributionBasedConfig distribution = new DistributionBasedConfig(Arrays.asList(3, 2, 2, 2));
        SimpleGraphRelationshipGenerator relationshipGenerator = new SimpleGraphRelationshipGenerator(distribution);

        GeneratorConfiguration config = new BasicGeneratorConfig(relationshipGenerator, nodeCreator, relationshipCreator);

        new Neo4jGraphGenerator(db).generateGraph(config);
    }
}
