package apoc.path;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.traversal.Evaluation;
import org.neo4j.graphdb.traversal.Evaluator;
import org.neo4j.graphdb.traversal.Evaluators;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Static factory methods for obtaining node evaluators
 */
public final class NodeEvaluators {
    // non-instantiable
    private NodeEvaluators() {};

    /**
     * Returns an evaluator which handles end nodes and terminator nodes
     * Returns null if both lists are empty
     */
    public static Evaluator endAndTerminatorNodeEvaluator(boolean filterStartNode, int minLevel, List<Node> endNodes, List<Node> terminatorNodes) {
        Evaluator endNodeEvaluator = null;
        Evaluator terminatorNodeEvaluator = null;

        if (!endNodes.isEmpty()) {
            Node[] nodes = endNodes.toArray(new Node[endNodes.size()]);
            endNodeEvaluator = Evaluators.includeWhereEndNodeIs(nodes);
        }

        if (!terminatorNodes.isEmpty()) {
            Node[] nodes = terminatorNodes.toArray(new Node[terminatorNodes.size()]);
            terminatorNodeEvaluator = Evaluators.pruneWhereEndNodeIs(nodes);
        }

        if (endNodeEvaluator != null || terminatorNodeEvaluator != null) {
            return new EndAndTerminatorNodeEvaluator(filterStartNode, minLevel, endNodeEvaluator, terminatorNodeEvaluator);
        }

        return null;
    }

    public static Evaluator whitelistNodeEvaluator(boolean filterStartNode, int minLevel, List<Node> whitelistNodes) {
        return new WhitelistNodeEvaluator(filterStartNode, minLevel, whitelistNodes);
    }

    public static Evaluator blacklistNodeEvaluator(boolean filterStartNode, int minLevel, List<Node> blacklistNodes) {
        return new BlacklistNodeEvaluator(filterStartNode, minLevel, blacklistNodes);
    }

    // The evaluators from pruneWhereEndNodeIs and includeWhereEndNodeIs interfere with each other, this makes them play nice
    private static class EndAndTerminatorNodeEvaluator implements Evaluator {
        private boolean filterStartNode;
        private int minLevel;
        private Evaluator endNodeEvaluator;
        private Evaluator terminatorNodeEvaluator;

        public EndAndTerminatorNodeEvaluator(boolean filterStartNode, int minLevel, Evaluator endNodeEvaluator, Evaluator terminatorNodeEvaluator) {
            this.filterStartNode = filterStartNode;
            this.minLevel = minLevel;
            this.endNodeEvaluator = endNodeEvaluator;
            this.terminatorNodeEvaluator = terminatorNodeEvaluator;
        }

        @Override
        public Evaluation evaluate(Path path) {
            if ((path.length() == 0 && !filterStartNode) || path.length() < minLevel) {
                return Evaluation.EXCLUDE_AND_CONTINUE;
            }

            // at least one has to give a thumbs up to include
            boolean includes = evalIncludes(endNodeEvaluator, path) || evalIncludes(terminatorNodeEvaluator, path);
            // prune = terminatorNodeEvaluator != null && !terminatorNodeEvaluator.evaluate(path).continues()
            // negate this to get continues result
            boolean continues = terminatorNodeEvaluator == null || terminatorNodeEvaluator.evaluate(path).continues();

            return Evaluation.of(includes, continues);
        }

        private boolean evalIncludes(Evaluator eval, Path path) {
            return eval != null && eval.evaluate(path).includes();
        }
    }

    private static class BlacklistNodeEvaluator extends PathExpanderNodeEvaluator {
        private Set<Node> blacklistSet;

        public BlacklistNodeEvaluator(boolean filterStartNode, int minLevel, List<Node> blacklistNodes) {
            super(filterStartNode, minLevel);
            blacklistSet = new HashSet<>(blacklistNodes);
        }

        @Override
        public Evaluation evaluate(Path path) {
            return path.length() == 0 && !filterStartNode ? Evaluation.INCLUDE_AND_CONTINUE :
                    blacklistSet.contains(path.endNode()) ? Evaluation.EXCLUDE_AND_PRUNE : Evaluation.INCLUDE_AND_CONTINUE;
        }
    }

    private static class WhitelistNodeEvaluator extends PathExpanderNodeEvaluator {
        private Set<Node> whitelistSet;

        public WhitelistNodeEvaluator(boolean filterStartNode, int minLevel, List<Node> whitelistNodes) {
            super(filterStartNode, minLevel);
            whitelistSet = new HashSet<>(whitelistNodes);
        }

        @Override
        public Evaluation evaluate(Path path) {
            return (path.length() == 0 && !filterStartNode) ? Evaluation.INCLUDE_AND_CONTINUE :
            whitelistSet.contains(path.endNode()) ? Evaluation.INCLUDE_AND_CONTINUE : Evaluation.EXCLUDE_AND_PRUNE;
        }
    }

    private static abstract class PathExpanderNodeEvaluator implements Evaluator {
        public final boolean filterStartNode;
        public final int minLevel;

        private PathExpanderNodeEvaluator(boolean filterStartNode, int minLevel) {
            this.filterStartNode = filterStartNode;
            this.minLevel = minLevel;
        }
    }
}
