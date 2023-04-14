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
 * {@link RelationshipGeneratorConfig} for {@link apoc.generate.relationship.BarabasiAlbertRelationshipGenerator}.
 * <p/>
 * Permitted values: 0 &lt; edgesPerNode &lt; numberOfNodes
 * Recommended values: Interested in phenomenological model? Use low edgesPerNode value (2 ~ 3)
 * Real nets can have more than that. Usually choose less than half of a "mean" degree.
 * Precision is not crucial here.
 */
public class BarabasiAlbertConfig extends NumberOfNodesBasedConfig {

    /**
     * Number of edges added to the graph when
     * a new node is connected. The node has this
     * number of edges at that instant.
     */
    private final int edgesPerNewNode;

    /**
     * Construct a new config.
     *
     * @param numberOfNodes   number of nodes in the network.
     * @param edgesPerNewNode number of edges per newly added node.
     */
    public BarabasiAlbertConfig(int numberOfNodes, int edgesPerNewNode) {
        super(numberOfNodes);
        this.edgesPerNewNode = edgesPerNewNode;
    }

    /**
     * Get the number of edges per newly added node.
     *
     * @return number of edges.
     */
    public int getEdgesPerNewNode() {
        return edgesPerNewNode;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isValid() {
        return super.isValid() && !(edgesPerNewNode < 1 || edgesPerNewNode + 1 > getNumberOfNodes());
    }
}
