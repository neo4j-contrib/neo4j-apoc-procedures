package apoc.generate;

import apoc.generate.config.GeneratorConfiguration;

/**
 * Component that generates a graph based on {@link GeneratorConfiguration}.
 */
public interface GraphGenerator {

    /**
     * Generate a graph.
     *
     * @param configuration for the graph generation.
     */
    void generateGraph(GeneratorConfiguration configuration);
}
