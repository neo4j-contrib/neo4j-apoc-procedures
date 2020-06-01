package apoc.generate.config;

/**
 * Configuration for a {@link apoc.generate.relationship.RelationshipGenerator}.
 */
public interface RelationshipGeneratorConfig {

    /**
     * Get the number of nodes that need to be created before the relationships can be generated and created.
     *
     * @return number of nodes for this configuration.
     */
    int getNumberOfNodes();

    /**
     * @return true if the config is valid.
     */
    boolean isValid();
}
