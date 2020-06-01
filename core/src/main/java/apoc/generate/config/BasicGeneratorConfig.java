package apoc.generate.config;

import apoc.generate.node.NodeCreator;
import apoc.generate.relationship.RelationshipCreator;
import apoc.generate.relationship.RelationshipGenerator;

/**
 * Basic implementation of {@link GeneratorConfiguration} where everything can be configured by constructor instantiation,
 * except for batch size, which defaults to 1000.
 */
public class BasicGeneratorConfig implements GeneratorConfiguration {

    private final RelationshipGenerator relationshipGenerator;
    private final NodeCreator nodeCreator;
    private final RelationshipCreator relationshipCreator;

    /**
     * Create a new configuration.
     *
     * @param relationshipGenerator core component, generating the edges.
     * @param nodeCreator           component capable of creating nodes.
     * @param relationshipCreator   component capable of creating edges.
     */
    public BasicGeneratorConfig(RelationshipGenerator relationshipGenerator, NodeCreator nodeCreator, RelationshipCreator relationshipCreator) {
        this.relationshipGenerator = relationshipGenerator;
        this.nodeCreator = nodeCreator;
        this.relationshipCreator = relationshipCreator;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getNumberOfNodes() {
        return relationshipGenerator.getNumberOfNodes();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NodeCreator getNodeCreator() {
        return nodeCreator;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public RelationshipCreator getRelationshipCreator() {
        return relationshipCreator;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public RelationshipGenerator getRelationshipGenerator() {
        return relationshipGenerator;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getBatchSize() {
        return 1000;
    }
}
