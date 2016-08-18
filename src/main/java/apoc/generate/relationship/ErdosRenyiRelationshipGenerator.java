package apoc.generate.relationship;

import apoc.generate.config.ErdosRenyiConfig;
import org.neo4j.helpers.collection.Pair;

import java.util.*;

/**
 * {@link RelationshipGenerator} implementation according to Erdos-Renyi random graphs. These are a basic class of
 * random graphs with exponential cut-off. A phase transition from many components graph to a completely connected graph
 * is present.
 * <p/>
 * The algorithm has a switch from sparse ER graph to dense ER graph generator. The sparse algorithm is based on
 * trial-correction method as suggested in the paper cited below. This is extremely inefficient for nearly-complete
 * graphs. The dense algorithm (written by GraphAware) is based on careful avoiding of edge indices in the selection.
 * <p/>
 * The switch allows to generate even complete graphs (eg. (V, E) = (20, 190) in a reasonable time. The switch is turned
 * on to dense graph generator for the case when number of edges requested is greater than half of total possible edges
 * that could be generated.
 */
public class ErdosRenyiRelationshipGenerator extends BaseRelationshipGenerator<ErdosRenyiConfig> {

    private final Random random = new Random();

    /**
     * Construct a new generator.
     *
     * @param configuration of the generator.
     */
    public ErdosRenyiRelationshipGenerator(ErdosRenyiConfig configuration) {
        super(configuration);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected List<Pair<Integer, Integer>> doGenerateEdges() {
        long threshold = getConfiguration().getNumberOfEdges() * 4;
        long potentialEdges = getConfiguration().getNumberOfNodes() * (getConfiguration().getNumberOfNodes() - 1);

        if (threshold > potentialEdges) {
            return doGenerateEdgesWithOmitList(); // Make sure to avoid edges
        }

        return doGenerateEdgesSimpler(); // Be more heuristic (pajek implementation using HashSet).
    }

    /**
     * This algorithm is implemented as recommended in
     * <p/>
     * Efficient generation of large random networks
     * by Vladimir Batagelj and Ulrik Brandes
     * <p/>
     * PHYSICAL REVIEW E 71, 036113, 2005
     *
     * @return edge list
     */
    private List<Pair<Integer, Integer>> doGenerateEdgesSimpler() {
        final int numberOfNodes = getConfiguration().getNumberOfNodes();
        final long numberOfEdges = getConfiguration().getNumberOfEdges();

        final Set<Pair<Integer, Integer>> edges = new HashSet<>();

        while (edges.size() < numberOfEdges) {
            int origin = random.nextInt(numberOfNodes);
            int target = random.nextInt(numberOfNodes);

            if (target == origin) {
                continue;
            }

            edges.add(Pair.of(origin, target));
        }

        return new LinkedList<>(edges);
    }

    /**
     * Improved implementation of Erdos-Renyi generator based on bijection from
     * edge labels to edge realisations. Works very well for large number of nodes,
     * but is slow with increasing number of edges. Best for denser networks, with
     * a clear giant component.
     *
     * @return edge list
     */
    private List<Pair<Integer, Integer>> doGenerateEdgesWithOmitList() {
        final int numberOfNodes = getConfiguration().getNumberOfNodes();
        final int numberOfEdges = getConfiguration().getNumberOfEdges();
        final long maxEdges = numberOfNodes * (numberOfNodes - 1) / 2;

        final List<Pair<Integer, Integer>> edges = new LinkedList<>();

        for (Long index : edgeIndices(numberOfEdges, maxEdges)) {
            edges.add(indexToEdgeBijection(index));
        }

        return edges;
    }

    /**
     * Maps an index in a hypothetical list of all edges to the actual edge.
     *
     * @param index index
     * @return an edge based on its unique label
     */
    private Pair<Integer, Integer> indexToEdgeBijection(long index) {
        long i = (long) Math.ceil((Math.sqrt(1 + 8 * (index + 1)) - 1) / 2);
        long diff = index + 1 - (i * (i - 1)) / 2;

        return Pair.of((int) i, (int) diff - 1);
    }

    private Set<Long> edgeIndices(int numberOfEdges, long maxEdges) {
        Set<Long> result = new HashSet<>(numberOfEdges);
        while (result.size() < numberOfEdges) {
            result.add(nextLong(maxEdges));
        }
        return result;
    }

    private long nextLong(long length) {
        return (long) (random.nextDouble() * length);
    }
}
