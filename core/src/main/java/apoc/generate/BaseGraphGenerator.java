package apoc.generate;

import apoc.generate.config.GeneratorConfiguration;

import java.util.List;

/**
 * Base class for {@link GraphGenerator} implementations.
 */
public abstract class BaseGraphGenerator implements GraphGenerator {

    /**
     * {@inheritDoc}
     */
    @Override
    public void generateGraph(GeneratorConfiguration configuration) {
        generateRelationships(configuration, generateNodes(configuration));
    }

    /**
     * Generate (i.e. create and persist) nodes.
     *
     * @param configuration generator config.
     * @return list of node IDs of the generated nodes.
     */
    protected abstract List<Long> generateNodes(GeneratorConfiguration configuration);

    /**
     * Generate (i.e. create and persist) relationships.
     *
     * @param config generator config.
     * @param nodes  list of node IDs of the generated nodes.
     */
    protected abstract void generateRelationships(final GeneratorConfiguration config, List<Long> nodes);
}
