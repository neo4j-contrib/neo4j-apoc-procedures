package apoc.generate.relationship;

import apoc.generate.config.InvalidConfigException;
import apoc.generate.config.RelationshipGeneratorConfig;
import org.neo4j.helpers.collection.Pair;

import java.util.List;

/**
 * A component that generates relationships based on a given {@link RelationshipGeneratorConfig}.
 */
public interface RelationshipGenerator {

    /**
     * Get the number of nodes that need to be created before the relationships can be generated and created.
     *
     * @return number of nodes this generator needs.
     */
    int getNumberOfNodes();

    /**
     * Generate edges (relationships) based on a degree distribution.
     *
     * @return pairs of node IDs representing edges.
     * @throws InvalidConfigException in case the given distribution is invalid for the generator implementation.
     */
    List<Pair<Integer, Integer>> generateEdges() throws InvalidConfigException;
}
