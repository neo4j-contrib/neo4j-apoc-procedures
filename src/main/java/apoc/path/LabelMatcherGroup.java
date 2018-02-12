package apoc.path;

import org.neo4j.graphdb.Node;

/**
 * A matcher for evaluating whether or not a node has at least one of the whitelisted labels (and does not have any blacklisted label).
 * Unlike a LabelMatcher, LabelMatcherGroups interpret context for labels according to filter symbols provided.
 * Labels can be added that are prefixed with filter symbols (+, -, /, >) (for whitelist, blacklist, terminator, and end node respectively).
 * Lack of a symbol is interpreted as whitelisted. Labels provided with the endnode and terminator filter symbols are also considered whitelisted.
 * The group as a whole carries the status of terminator node or end node...if any label added has the appropriate filter symbol, the appropriate flag is toggled on for the entire group,
 * which will be reflected in `isEndNode()` and `isTerminatorNode()`.
 * Because of this, the endnode and terminator filter symbols can be prefixed in combination with a whitelist or blacklist symbol.
 * For example, '>-EXCLUDED' means that there is a blacklist on any node with the label 'EXCLUDED', but if not caught by the blacklist the
 * node is marked as an endnode.
 * If no labels are set as whitelisted, then all labels are considered whitelisted (if not otherwise disallowed by the blacklist).
 */
public class LabelMatcherGroup {
    private boolean isEndNode;
    private boolean isTerminatorNode;
    private LabelMatcher whitelistMatcher = new LabelMatcher();
    private LabelMatcher blacklistMatcher = new LabelMatcher();

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

            char operator = filterString.charAt(0);
            if (operator == '>') {
                isEndNode = true;
                filterString = filterString.substring(1);
            } else if (operator == '/') {
                isTerminatorNode = true;
                filterString = filterString.substring(1);
            }

            LabelMatcher matcher;

            if (filterString.charAt(0) == '-') {
                matcher = blacklistMatcher;
                filterString = filterString.substring(1);
            } else if (filterString.charAt(0) == '+') {
                matcher = whitelistMatcher;
                filterString = filterString.substring(1);
            } else {
                matcher = whitelistMatcher;
            }

            matcher.addLabel(filterString);
        }

        return this;
    }

    public boolean matchesLabels(Node node) {
        if (blacklistMatcher.matchesLabels(node)) {
            return false;
        }

        // empty whitelist is equivalent to everything whitelisted (unless caught by blacklist above)
        if (whitelistMatcher.isEmpty() || whitelistMatcher.matchesLabels(node)) {
            return true;
        }

        return false;
    }

    public boolean isEndNode() {
        return isEndNode;
    }

    public boolean isTerminatorNode() {
        return isTerminatorNode;
    }
}