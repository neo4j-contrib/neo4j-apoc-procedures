package apoc.generate.relationship;

import apoc.generate.config.NumberOfNodesBasedConfig;
import org.neo4j.helpers.collection.Pair;

import java.util.ArrayList;
import java.util.List;

/**
 * {@link RelationshipGenerator} that generates a complete (undirected) graph.
 * Used for the core graph in {@link BarabasiAlbertRelationshipGenerator}.
 */
public class CompleteGraphRelationshipGenerator extends BaseRelationshipGenerator<NumberOfNodesBasedConfig> {

    /**
     * Create a new generator.
     *
     * @param configuration of the generator.
     */
    public CompleteGraphRelationshipGenerator(NumberOfNodesBasedConfig configuration) {
        super(configuration);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected List<Pair<Integer, Integer>> doGenerateEdges() {
        List<Pair<Integer, Integer>> graph = new ArrayList<>();

        // Create a completely connected undirected network
        for (int i = 0; i < getConfiguration().getNumberOfNodes(); i++) {
            for (int j = i + 1; j < getConfiguration().getNumberOfNodes(); j++) {
                graph.add(Pair.of(i, j));
            }
        }

        return graph;
    }
}
