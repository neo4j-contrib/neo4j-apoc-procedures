package apoc.generate.config;

import java.math.BigInteger;

/**
 * {@link RelationshipGeneratorConfig} for {@link apoc.generate.relationship.ErdosRenyiRelationshipGenerator}.
 *
 * numberOfNodes: number of nodes in the graph. 1 < numberOfNodes
 * numberOfEdges: number of edges present in the generated graph. Range: 0 < numberOfEdges < (numberOfNodes*(numberOfNodes - 1)/2).
 *                Highly recommended not to exceed ~ 4 000 000 edges.
 *
 */
public class ErdosRenyiConfig extends NumberOfNodesBasedConfig {

    private final int numberOfEdges;

    /**
     * Constructs a new config.
     *
     * @param numberOfNodes number of nodes present in the network.
     * @param numberOfEdges number of edges present in the network.
     */
    public ErdosRenyiConfig(int numberOfNodes, int numberOfEdges) {
        super(numberOfNodes);
        this.numberOfEdges = numberOfEdges;
    }

    /**
     * Get the number of edges present in the network.
     *
     * @return number of edges.
     */
    public int getNumberOfEdges() {
        return numberOfEdges;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isValid() {
        BigInteger maxNumberOfEdges = BigInteger.valueOf(getNumberOfNodes());
        maxNumberOfEdges = maxNumberOfEdges.multiply(maxNumberOfEdges.subtract(BigInteger.ONE));
        maxNumberOfEdges = maxNumberOfEdges.divide(BigInteger.valueOf(2));

        return super.isValid() && numberOfEdges > 0 && BigInteger.valueOf(numberOfEdges).compareTo(maxNumberOfEdges) < 1;
    }
}
