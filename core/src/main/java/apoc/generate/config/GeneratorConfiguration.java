package apoc.generate.config;

import apoc.generate.node.NodeCreator;
import apoc.generate.relationship.RelationshipCreator;
import apoc.generate.relationship.RelationshipGenerator;

/**
 * A configuration of a {@link apoc.generate.GraphGenerator}.
 */
public interface GeneratorConfiguration {

    /**
     * Get the total number of nodes that will be generated.
     *
     * @return number of nodes.
     */
    int getNumberOfNodes();

    /**
     * Get the component generating relationships.
     *
     * @return relationship generator.
     */
    RelationshipGenerator getRelationshipGenerator();

    /**
     * Get the component creating (populating) nodes.
     *
     * @return node creator.
     */
    NodeCreator getNodeCreator();

    /**
     * Get the component creating (populating) relationships.
     *
     * @return relationship creator.
     */
    RelationshipCreator getRelationshipCreator();

    /**
     * Get the no. nodes/relationships created in a single transaction.
     *
     * @return batch size.
     */
    int getBatchSize();
}
