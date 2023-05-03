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
package apoc.generate.config;

/**
 * {@link RelationshipGeneratorConfig} for {@link apoc.generate.relationship.WattsStrogatzRelationshipGenerator}.
 *
 * meanDegree: degree which a node in the graph has on average (best to choose something &lt; 10)
 * numberOfNodes: number of nodes present in the graph
 * beta: probability an edge will be rewired. Rewiring means that an edge is removed and replaced by another edge
 *       created from a pair chosen at random from a set of unconnected node pairs. Controls the clustering of the graph.
 *       beta = 1.0: Erdos-Renyi model
 *       beta = 0.0: Ring graph
 *       0.0 &lt; beta &lt; 1.0: Fast convergence towards a random graph, but still sufficiently clustered.
 *
 * Recommended value of beta to exploit typical (randomness &amp; clustering) properties of the W-S model: 0.4 &lt; beta &lt; 0.6
 */
public class WattsStrogatzConfig extends NumberOfNodesBasedConfig {

    private final int meanDegree;
    private final double beta;

    /**
     * Construct a new config.
     *
     * @param numberOfNodes number of nodes in the network.
     * @param meanDegree    mean degree of the regular ring network constructed as an initial step for Watts-Strogatz.
     * @param beta          probability of edge rewiring at a given step.
     */
    public WattsStrogatzConfig(int numberOfNodes, int meanDegree, double beta) {
        super(numberOfNodes);
        this.meanDegree = meanDegree;
        this.beta = beta;
    }

    /**
     * Get the mean degree of nodes in the generated network.
     *
     * @return mean degree.
     */
    public int getMeanDegree() {
        return meanDegree;
    }

    /**
     * Get the probability of edge rewiring at each step.
     *
     * @return p.
     */
    public double getBeta() {
        return beta;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isValid() {
        return !(meanDegree % 2 != 0 ||
                meanDegree < 3 ||
                meanDegree > getNumberOfNodes() - 1) &&
                (0 <= beta && beta <= 1);
    }
}
