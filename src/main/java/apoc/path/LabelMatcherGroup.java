package apoc.path;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.traversal.Evaluation;

import static org.neo4j.graphdb.traversal.Evaluation.*;

/**
 * A matcher for evaluating whether or not a node is accepted by a group of matchers comprised of a blacklist, whitelist, endNode and termination node matchers.
 * Unlike a LabelMatcher, LabelMatcherGroups interpret context for labels according to filter symbols provided.
 * Labels can be added that are prefixed with filter symbols (+, -, /, &gt;) (for whitelist, blacklist, terminator, and end node respectively).
 * Lack of a symbol is interpreted as whitelisted.
 * If no labels are set as whitelisted, then all labels are considered whitelisted (if not otherwise disallowed by the blacklist).
 * The node will not be included if blacklisted, or not matched via the whitelist, end node, or termination node matchers.
 * If end nodes only, then the node will only be included if matched via the end node and termination node matchers.
 * The path will be pruned if matching the blacklist, the termination node matchers, or otherwise not included by any of the other matchers.
 */
public class LabelMatcherGroup {
    private boolean endNodesOnly;
    private LabelMatcher whitelistMatcher = new LabelMatcher();
    private LabelMatcher blacklistMatcher = new LabelMatcher();
    private LabelMatcher endNodeMatcher = new LabelMatcher();
    private LabelMatcher terminatorNodeMatcher = new LabelMatcher();

    public LabelMatcherGroup addLabels(String fullFilterString) {
        if (fullFilterString !=  null && !fullFilterString.isEmpty()) {
            String[] elements = fullFilterString.split("\\|");

            for (String filterString : elements) {
                addLabel(filterString);
            }
        }

        return this;
    }

    public LabelMatcherGroup addLabel(String filterString) {
        if (filterString !=  null && !filterString.isEmpty()) {
            LabelMatcher matcher;

            char operator = filterString.charAt(0);

            switch (operator) {
                case '>':
                    endNodesOnly = true;
                    matcher = endNodeMatcher;
                    filterString = filterString.substring(1);
                    break;
                case '/':
                    endNodesOnly = true;
                    matcher = terminatorNodeMatcher;
                    filterString = filterString.substring(1);
                    break;
                case '-':
                    matcher = blacklistMatcher;
                    filterString = filterString.substring(1);
                    break;
                case '+':
                    filterString = filterString.substring(1);
                default:
                    matcher = whitelistMatcher;
            }

            matcher.addLabel(filterString);
        }

        return this;
    }

    public Evaluation evaluate(Node node, boolean belowMinLevel) {
        if (blacklistMatcher.matchesLabels(node)) {
            return EXCLUDE_AND_PRUNE;
        }

        if (terminatorNodeMatcher.matchesLabels(node)) {
            return belowMinLevel ? EXCLUDE_AND_CONTINUE : INCLUDE_AND_PRUNE;
        }

        if (endNodeMatcher.matchesLabels(node)) {
            return belowMinLevel ? EXCLUDE_AND_CONTINUE : INCLUDE_AND_CONTINUE;
        }

        if (whitelistMatcher.isEmpty() || whitelistMatcher.matchesLabels(node)) {
            return endNodesOnly || belowMinLevel ? EXCLUDE_AND_CONTINUE : INCLUDE_AND_CONTINUE;
        }

        return EXCLUDE_AND_PRUNE;
    }

    public boolean isEndNodesOnly() {
        return endNodesOnly;
    }

    public void setEndNodesOnly(boolean endNodesOnly) {
        this.endNodesOnly = endNodesOnly;
    }
}
