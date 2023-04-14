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

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.traversal.Evaluation;
import org.neo4j.graphdb.traversal.Evaluator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.neo4j.graphdb.traversal.Evaluation.EXCLUDE_AND_CONTINUE;
import static org.neo4j.graphdb.traversal.Evaluation.INCLUDE_AND_CONTINUE;

// when no commas present, acts as a pathwide label filter
public class LabelSequenceEvaluator implements Evaluator {
    private List<LabelMatcherGroup> sequenceMatchers;

    private Evaluation whitelistAllowedEvaluation;
    private boolean endNodesOnly;
    private boolean filterStartNode;
    private boolean beginSequenceAtStart;
    private long minLevel = -1;

    public LabelSequenceEvaluator(String labelSequence, boolean filterStartNode, boolean beginSequenceAtStart, int minLevel) {
        List<String> labelSequenceList;

        // parse sequence
        if (labelSequence != null && !labelSequence.isEmpty()) {
            labelSequenceList = Arrays.asList(labelSequence.split(","));
        } else {
            labelSequenceList = Collections.emptyList();
        }

        initialize(labelSequenceList, filterStartNode, beginSequenceAtStart, minLevel);
    }

    public LabelSequenceEvaluator(List<String> labelSequenceList, boolean filterStartNode, boolean beginSequenceAtStart, int minLevel) {
        initialize(labelSequenceList, filterStartNode, beginSequenceAtStart, minLevel);
    }

    private void initialize(List<String> labelSequenceList, boolean filterStartNode, boolean beginSequenceAtStart, int minLevel) {
        this.filterStartNode = filterStartNode;
        this.beginSequenceAtStart = beginSequenceAtStart;
        this.minLevel = minLevel;
        sequenceMatchers = new ArrayList<>(labelSequenceList.size());

        for (String labelFilterString : labelSequenceList) {
            LabelMatcherGroup matcherGroup = new LabelMatcherGroup().addLabels(labelFilterString.trim());
            sequenceMatchers.add(matcherGroup);
            endNodesOnly = endNodesOnly || matcherGroup.isEndNodesOnly();
        }

        // if true for one matcher, need to set true for all matchers
        if (endNodesOnly) {
            for (LabelMatcherGroup group : sequenceMatchers) {
                group.setEndNodesOnly(endNodesOnly);
            }
        }

        whitelistAllowedEvaluation = endNodesOnly ? EXCLUDE_AND_CONTINUE : INCLUDE_AND_CONTINUE;
    }

    @Override
    public Evaluation evaluate(Path path) {
        int depth = path.length();
        Node node = path.endNode();
        boolean belowMinLevel = depth < minLevel;

        // if start node shouldn't be filtered, exclude/include based on if using termination/endnode filter or not
        // minLevel evaluator will separately enforce exclusion if we're below minLevel
        if (depth == 0 && (!filterStartNode || !beginSequenceAtStart)) {
            return whitelistAllowedEvaluation;
        }

        // the user may want the sequence to begin at the start node (default), or the sequence may only apply from the next node on
        LabelMatcherGroup matcherGroup = sequenceMatchers.get((beginSequenceAtStart ? depth : depth - 1) % sequenceMatchers.size());

        return matcherGroup.evaluate(node, belowMinLevel);
    }
}