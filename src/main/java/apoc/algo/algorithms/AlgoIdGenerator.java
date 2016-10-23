package apoc.algo.algorithms;

/**
 * @author mh
 * @since 21.10.16
 */
interface AlgoIdGenerator {
    int getAlgoNodeId(long node);

    int getOrCreateAlgoNodeId(long node);

    long getMappedNode(int algoId);

    int getNodeCount();
}
