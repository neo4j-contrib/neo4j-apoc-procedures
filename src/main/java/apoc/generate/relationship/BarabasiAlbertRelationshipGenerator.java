package apoc.generate.relationship;

import apoc.generate.config.BarabasiAlbertConfig;
import apoc.generate.config.NumberOfNodesBasedConfig;
import org.neo4j.helpers.collection.Pair;

import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

/**
 * {@link RelationshipGenerator} implemented according to the Barabasi-Albert preferential attachment model, which is
 * appropriate for networks reflecting cumulative advantage (rich get richer).
 * <p/>
 * Each newly added node has a probability weighted by the node degree to be attached. Since BA references
 * (Newmann, Barabasi-Albert) do not define strict conditions on initial state of the model, completely connected network
 * is used to start up the algorithm.
 */
public class BarabasiAlbertRelationshipGenerator extends BaseRelationshipGenerator<BarabasiAlbertConfig> {

    private final Random random = new Random();

    /**
     * Create a new generator.
     *
     * @param configuration of the generator.
     */
    public BarabasiAlbertRelationshipGenerator(BarabasiAlbertConfig configuration) {
        super(configuration);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected List<Pair<Integer, Integer>> doGenerateEdges() {
        final int edgesPerNewNode = getConfiguration().getEdgesPerNewNode();
        final long numberOfNodes = getConfiguration().getNumberOfNodes();

        // Create a completely connected network
        final List<Pair<Integer, Integer>> edges
                = new CompleteGraphRelationshipGenerator(new NumberOfNodesBasedConfig(edgesPerNewNode + 1)).doGenerateEdges();

        // Preferentially attach other nodes
        final Set<Integer> omit = new HashSet<>(edgesPerNewNode);
        for (int source = edgesPerNewNode + 1; source < numberOfNodes; source++) {
            omit.clear();

            for (int edge = 0; edge < edgesPerNewNode; edge++) {
                while (true) {
                    Pair<Integer, Integer> randomEdge = edges.get(random.nextInt(edges.size()));
                    int target = random.nextBoolean() ? randomEdge.first() : randomEdge.other();

                    if (omit.contains(target)) {
                        continue;
                    }

                    omit.add(target); // to avoid multi-edges

                    edges.add(Pair.of(target, source));

                    break;
                }
            }
        }

        return edges;
    }
}
