/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package apoc.path;

import java.util.*;
import java.util.stream.Collectors;

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
        public boolean matchesLabels(Set<String> nodeLabels) {
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

        // split any `:` char not preceded by `\`
        String[] elements = label.split("(?<!\\\\):");
        if (elements.length == 1) {
            label = sanitizeLabel(label);
            labels.add(label);
        } else if (elements.length > 1) {
            if (compoundLabels == null) {
                compoundLabels = new ArrayList<>();
            }

            List<String> elementsList = Arrays.stream(elements)
                    .map(this::sanitizeLabel)
                    .collect(Collectors.toList());
            compoundLabels.add(elementsList);
        }

        return this;
    }

    private String sanitizeLabel(String label) {
        // from `\:` to `:`
        return label.replaceAll("\\\\:", ":");
    }

    public boolean matchesLabels(Set<String> nodeLabels) {
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


