package apoc.generate.relationship;

import apoc.generate.config.InvalidConfigException;
import apoc.generate.config.RelationshipGeneratorConfig;
import org.neo4j.helpers.collection.Pair;

import java.util.List;

/**
 * Abstract base-class for {@link RelationshipGenerator} implementations.
 *
 * @param <T> type of accepted configuration.
 */
public abstract class BaseRelationshipGenerator<T extends RelationshipGeneratorConfig> implements RelationshipGenerator {

    private final T configuration;

    /**
     * Construct a new relationship generator.
     *
     * @param configuration to base the generation on
     */
    protected BaseRelationshipGenerator(T configuration) {
        this.configuration = configuration;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getNumberOfNodes() {
        return configuration.getNumberOfNodes();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Pair<Integer, Integer>> generateEdges() throws InvalidConfigException {
        if (!configuration.isValid()) {
            throw new InvalidConfigException("The supplied config is not valid");
        }

        return doGenerateEdges();
    }

    /**
     * Perform the actual edge generation.
     *
     * @return generated edges as pair of node IDs that should be connected.
     */
    protected abstract List<Pair<Integer, Integer>> doGenerateEdges();

    /**
     * Get the configuration of this generator.
     *
     * @return configuration.
     */
    protected T getConfiguration() {
        return configuration;
    }
}
