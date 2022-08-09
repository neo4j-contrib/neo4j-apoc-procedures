package apoc.generate.config;

import apoc.generate.distribution.SimpleDegreeDistribution;

import java.util.List;

/**
 * {@link SimpleDegreeDistribution} serving as a {@link RelationshipGeneratorConfig}.
 *
 * The sum of all degrees should be an even integer. Moreover, not all distributions
 * correspond to a simple, undirected graph. Only graphs that satisfy Erdos-Gallai condition
 * or equivalently the Havel-Hakimi test are valid. The distribution is tested in {@link SimpleDegreeDistribution} class.
 */
public class DistributionBasedConfig extends SimpleDegreeDistribution implements RelationshipGeneratorConfig {

    /**
     * Create a new config.
     *
     * @param degrees list of node degrees.
     */
    public DistributionBasedConfig(List<Integer> degrees) {
        super(degrees);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getNumberOfNodes() {
        return size();
    }
}
