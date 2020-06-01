package apoc.path;

import org.neo4j.graphdb.Node;

import java.util.*;

/**
 * A generic label matcher which evaluates whether or not a node has at least one of the labels added on the matcher.
 * String labels can be added on the matcher. The label can optionally be be prefixed with `:`.
 * Also handles compound labels (multiple labels separated by `:`), and a node will be matched if it has all of the labels
 * in a compound label (order does not matter).
 * If the node only has a subset of the compound label, it will only be matched if that subset is in the matcher.
 * For example, a LabelMatcher with only `Person:Manager` will only match on nodes with both :Person and :Manager, not just one or the other.
 * Any other labels on the matched node would not be relevant and would not affect the match.
 * If the LabelMatcher only had `Person:Manager` and `Person:Boss`, then only nodes with both :Person and :Manager, or :Person and :Boss, would match.
 * Some nodes that would not match would be: :Person, :Boss, :Manager, :Boss:Manager, but :Boss:Person:HeadHoncho would match fine.
 * Also accepts a special `*` label, indicating that the matcher will always return a positive match.
 * LabelMatchers hold no context about what a match means, and do not handle labels prefixed with filter symbols (+, -, /, &gt;).
 * Please strip these symbols from the start of each label before adding to the matcher.
 */
public class LabelMatcher {
    private List<String> labels = new ArrayList<>();
    private List<List<String>> compoundLabels;

    private static LabelMatcher ACCEPTS_ALL_LABEL_MATCHER = new LabelMatcher() {
        @Override
        public boolean matchesLabels(Node node) {
            return true;
        }

        @Override
        public LabelMatcher addLabel(String label) {
            return this; // no-op
        }

        @Override
        public boolean isEmpty() {
            return false;
        }
    };

    public static LabelMatcher acceptsAllLabelMatcher() {
        return ACCEPTS_ALL_LABEL_MATCHER;
    }

    public LabelMatcher addLabel(String label) {
        if ("*".equals(label)) {
            return ACCEPTS_ALL_LABEL_MATCHER;
        }

        if (label.charAt(0) == ':') {
            label = label.substring(1);
        }

        String[] elements = label.split(":");
        if (elements.length == 1) {
            labels.add(label);
        } else if (elements.length > 1) {
            if (compoundLabels == null) {
                compoundLabels = new ArrayList<>();
            }

            compoundLabels.add(Arrays.asList(elements));
        }

        return this;
    }

    public boolean matchesLabels(Node node) {
        Set<String> nodeLabels = new HashSet<>();
        node.getLabels().forEach(label -> nodeLabels.add(label.name()));

        for ( String label : labels ) {
            if (nodeLabels.contains(label)) {
                return true;
            }
        }

        if (compoundLabels != null) {
            for (List<String> compoundLabel : compoundLabels) {
                if (nodeLabels.containsAll(compoundLabel)) {
                    return true;
                }
            }
        }

        return false;
    }

    public boolean isEmpty() {
        return labels.isEmpty() && (compoundLabels == null || compoundLabels.isEmpty());
    }
}


