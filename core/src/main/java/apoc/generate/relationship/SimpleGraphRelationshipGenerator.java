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
package apoc.generate.relationship;

import apoc.generate.config.DistributionBasedConfig;
import apoc.generate.distribution.MutableDegreeDistribution;
import apoc.generate.distribution.MutableSimpleDegreeDistribution;
import apoc.generate.utils.WeightedReservoirSampler;
import java.util.ArrayList;
import java.util.List;
import org.neo4j.internal.helpers.collection.Pair;

/**
 * A simple minded {@link RelationshipGenerator} based on a {@link apoc.generate.distribution.DegreeDistribution}
 * <p/>
 * Please note that the distribution of randomly generated graphs isn't exactly uniform (see the paper below)
 * <p/>
 * Uses Blitzstein-Diaconis algorithm Ref:
 * <p/>
 * A SEQUENTIAL IMPORTANCE SAMPLING ALGORITHM FOR GENERATING RANDOM GRAPHS WITH PRESCRIBED DEGREES
 * By Joseph Blitzstein and Persi Diaconis (Stanford University). (Harvard, June 2006)
 */
public class SimpleGraphRelationshipGenerator extends BaseRelationshipGenerator<DistributionBasedConfig> {

    public SimpleGraphRelationshipGenerator(DistributionBasedConfig configuration) {
        super(configuration);
    }

    /**
     * {@inheritDoc}
     * <p/>
     * Returns an edge-set corresponding to a randomly chosen simple graph.
     */
    @Override
    protected List<Pair<Integer, Integer>> doGenerateEdges() {
        List<Pair<Integer, Integer>> edges = new ArrayList<>();
        MutableDegreeDistribution distribution =
                new MutableSimpleDegreeDistribution(getConfiguration().getDegrees());

        while (!distribution.isZeroList()) {
            // int length = distribution.size();
            int index = 0;
            long min = Long.MAX_VALUE;

            // find minimal nonzero element
            for (int i = 0; i < distribution.size(); ++i) {
                long elem = distribution.get(i);
                if (elem != 0 && elem < min) {
                    min = elem;
                    index = i;
                }
            }

            WeightedReservoirSampler sampler = new WeightedReservoirSampler();

            // Obtain a candidate list:
            while (true) {
                MutableDegreeDistribution temp = new MutableSimpleDegreeDistribution(distribution.getDegrees());
                int candidateIndex = sampler.randomIndexChoice(temp.getDegrees(), index);

                Pair<Integer, Integer> edgeCandidate = Pair.of(candidateIndex, index);

                //  Checks if edge has already been added.
                boolean skip = false;
                for (Pair<Integer, Integer> edge : edges) {
                    if (edge.equals(edgeCandidate)) {
                        skip = true;
                        break;
                    }
                }

                if (skip) {
                    continue;
                }

                // Prepare the candidate set and test if it is graphical
                temp.decrease(index);
                temp.decrease(candidateIndex);

                if (temp.isValid()) {
                    distribution = temp;
                    edges.add(edgeCandidate); // edge is allowed, add it.
                    break;
                }
            }
        }

        return edges;
    }
}
