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

import apoc.generate.config.InvalidConfigException;
import apoc.generate.config.RelationshipGeneratorConfig;
import org.neo4j.internal.helpers.collection.Pair;

import java.util.List;

/**
 * A component that generates relationships based on a given {@link RelationshipGeneratorConfig}.
 */
public interface RelationshipGenerator {

    /**
     * Get the number of nodes that need to be created before the relationships can be generated and created.
     *
     * @return number of nodes this generator needs.
     */
    int getNumberOfNodes();

    /**
     * Generate edges (relationships) based on a degree distribution.
     *
     * @return pairs of node IDs representing edges.
     * @throws InvalidConfigException in case the given distribution is invalid for the generator implementation.
     */
    List<Pair<Integer, Integer>> generateEdges() throws InvalidConfigException;
}
