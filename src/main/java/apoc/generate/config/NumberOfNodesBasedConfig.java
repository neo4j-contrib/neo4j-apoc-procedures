package apoc.generate.config;

/**
 * {@link RelationshipGeneratorConfig} that is based on an explicitly defined number of nodes in the network.
 */
public class NumberOfNodesBasedConfig implements RelationshipGeneratorConfig {

    private final int numberOfNodes;

    /**
     * Construct a new config.
     *
     * @param numberOfNodes number of nodes present in the network.
     */
    public NumberOfNodesBasedConfig(int numberOfNodes) {
        this.numberOfNodes = numberOfNodes;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getNumberOfNodes() {
        return numberOfNodes;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isValid() {
        return numberOfNodes >= 2;
    }
}
