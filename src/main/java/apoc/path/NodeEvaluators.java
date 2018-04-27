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
    public static Evaluator endAndTerminatorNodeEvaluator(List<Node> endNodes, List<Node> terminatorNodes) {
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
            return new EndAndTerminatorNodeEvaluator(endNodeEvaluator, terminatorNodeEvaluator);
        }

        return null;
    }

    public static Evaluator whitelistNodeEvaluator(List<Node> whitelistNodes) {
        return new WhitelistNodeEvaluator(whitelistNodes);
    }

    public static Evaluator blacklistNodeEvaluator(List<Node> blacklistNodes) {
        return new BlacklistNodeEvaluator(blacklistNodes);
    }

    // The evaluators from pruneWhereEndNodeIs and includeWhereEndNodeIs interfere with each other, this makes them play nice
    private static class EndAndTerminatorNodeEvaluator implements Evaluator {
        private Evaluator endNodeEvaluator;
        private Evaluator terminatorNodeEvaluator;

        public EndAndTerminatorNodeEvaluator(Evaluator endNodeEvaluator, Evaluator terminatorNodeEvaluator) {
            this.endNodeEvaluator = endNodeEvaluator;
            this.terminatorNodeEvaluator = terminatorNodeEvaluator;
        }

        @Override
        public Evaluation evaluate(Path path) {
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

    private static class BlacklistNodeEvaluator implements Evaluator {
        private Set<Node> blacklistSet;

        public BlacklistNodeEvaluator(List<Node> blacklistNodes) {
            blacklistSet = new HashSet<>(blacklistNodes);
        }

        @Override
        public Evaluation evaluate(Path path) {
            return blacklistSet.contains(path.endNode()) ? Evaluation.EXCLUDE_AND_PRUNE : Evaluation.INCLUDE_AND_CONTINUE;
        }
    }

    private static class WhitelistNodeEvaluator implements Evaluator {
        private Set<Node> whitelistSet;

        public WhitelistNodeEvaluator(List<Node> whitelistNodes) {
            whitelistSet = new HashSet<>(whitelistNodes);
        }

        @Override
        public Evaluation evaluate(Path path) {
            return whitelistSet.contains(path.endNode()) ? Evaluation.INCLUDE_AND_CONTINUE : Evaluation.EXCLUDE_AND_PRUNE;
        }
    }
}
