package apoc.generate.config;

/**
 * {@link RelationshipGeneratorConfig} for {@link apoc.generate.relationship.BarabasiAlbertRelationshipGenerator}.
 * <p/>
 * Permitted values: 0 < edgesPerNode < numberOfNodes
 * Recommended values: Interested in phenomenological model? Use low edgesPerNode value (2 ~ 3)
 * Real nets can have more than that. Usually choose less than half of a "mean" degree.
 * Precision is not crucial here.
 */
public class BarabasiAlbertConfig extends NumberOfNodesBasedConfig {

    /**
     * Number of edges added to the graph when
     * a new node is connected. The node has this
     * number of edges at that instant.
     */
    private final int edgesPerNewNode;

    /**
     * Construct a new config.
     *
     * @param numberOfNodes   number of nodes in the network.
     * @param edgesPerNewNode number of edges per newly added node.
     */
    public BarabasiAlbertConfig(int numberOfNodes, int edgesPerNewNode) {
        super(numberOfNodes);
        this.edgesPerNewNode = edgesPerNewNode;
    }

    /**
     * Get the number of edges per newly added node.
     *
     * @return number of edges.
     */
    public int getEdgesPerNewNode() {
        return edgesPerNewNode;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isValid() {
        return super.isValid() && !(edgesPerNewNode < 1 || edgesPerNewNode + 1 > getNumberOfNodes());
    }
}
