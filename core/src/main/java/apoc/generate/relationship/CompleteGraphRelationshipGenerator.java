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

import apoc.generate.config.NumberOfNodesBasedConfig;
import org.neo4j.internal.helpers.collection.Pair;

import java.util.ArrayList;
import java.util.List;

/**
 * {@link RelationshipGenerator} that generates a complete (undirected) graph.
 * Used for the core graph in {@link BarabasiAlbertRelationshipGenerator}.
 */
public class CompleteGraphRelationshipGenerator extends BaseRelationshipGenerator<NumberOfNodesBasedConfig> {

    /**
     * Create a new generator.
     *
     * @param configuration of the generator.
     */
    public CompleteGraphRelationshipGenerator(NumberOfNodesBasedConfig configuration) {
        super(configuration);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected List<Pair<Integer, Integer>> doGenerateEdges() {
        List<Pair<Integer, Integer>> graph = new ArrayList<>();

        // Create a completely connected undirected network
        for (int i = 0; i < getConfiguration().getNumberOfNodes(); i++) {
            for (int j = i + 1; j < getConfiguration().getNumberOfNodes(); j++) {
                graph.add(Pair.of(i, j));
            }
        }

        return graph;
    }
}
