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

import apoc.generate.node.NodeCreator;
import apoc.generate.relationship.RelationshipCreator;
import apoc.generate.relationship.RelationshipGenerator;

/**
 * A configuration of a {@link apoc.generate.GraphGenerator}.
 */
public interface GeneratorConfiguration {

    /**
     * Get the total number of nodes that will be generated.
     *
     * @return number of nodes.
     */
    int getNumberOfNodes();

    /**
     * Get the component generating relationships.
     *
     * @return relationship generator.
     */
    RelationshipGenerator getRelationshipGenerator();

    /**
     * Get the component creating (populating) nodes.
     *
     * @return node creator.
     */
    NodeCreator getNodeCreator();

    /**
     * Get the component creating (populating) relationships.
     *
     * @return relationship creator.
     */
    RelationshipCreator getRelationshipCreator();

    /**
     * Get the no. nodes/relationships created in a single transaction.
     *
     * @return batch size.
     */
    int getBatchSize();
}
