package apoc.generate;

import apoc.generate.config.GeneratorConfiguration;
import apoc.generate.relationship.RelationshipGenerator;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Pair;

import java.util.ArrayList;
import java.util.List;

/**
 * {@link GraphGenerator} for Neo4j.
 */
public class Neo4jGraphGenerator extends BaseGraphGenerator {

    private final GraphDatabaseService database;

    public Neo4jGraphGenerator(GraphDatabaseService database) {
        this.database = database;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected List<Long> generateNodes(final GeneratorConfiguration config) {
        final List<Long> nodes = new ArrayList<>();

        int numberOfNodes = config.getNumberOfNodes();

        Transaction tx = database.beginTx();
        try {
            for (int i = 1; i <= numberOfNodes; i++) {
                nodes.add(config.getNodeCreator().createNode(tx).getId());

                if (i % config.getBatchSize() == 0) {
                    tx.commit();
                    tx.close();
                    tx = database.beginTx();
                }
            }
            tx.commit();
        } finally {
            tx.close();
        }

        return nodes;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void generateRelationships(final GeneratorConfiguration config, final List<Long> nodes) {
        RelationshipGenerator relationshipGenerator = config.getRelationshipGenerator();
        List<Pair<Integer, Integer>> relationships = relationshipGenerator.generateEdges();

        Transaction tx = database.beginTx();
        try {
            int i = 0;
            for (Pair<Integer, Integer> input : relationships) {
                Node first = tx.getNodeById(nodes.get(input.first()));
                Node second = tx.getNodeById(nodes.get(input.other()));
                config.getRelationshipCreator().createRelationship(first, second);

                if (++i % config.getBatchSize() == 0) {
                    tx.commit();
                    tx.close();
                    tx = database.beginTx();
                }
            }

            tx.commit();
        } finally {
            tx.close();
        }
    }
}
