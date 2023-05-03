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
 * Basic implementation of {@link GeneratorConfiguration} where everything can be configured by constructor instantiation,
 * except for batch size, which defaults to 1000.
 */
public class BasicGeneratorConfig implements GeneratorConfiguration {

    private final RelationshipGenerator relationshipGenerator;
    private final NodeCreator nodeCreator;
    private final RelationshipCreator relationshipCreator;

    /**
     * Create a new configuration.
     *
     * @param relationshipGenerator core component, generating the edges.
     * @param nodeCreator           component capable of creating nodes.
     * @param relationshipCreator   component capable of creating edges.
     */
    public BasicGeneratorConfig(RelationshipGenerator relationshipGenerator, NodeCreator nodeCreator, RelationshipCreator relationshipCreator) {
        this.relationshipGenerator = relationshipGenerator;
        this.nodeCreator = nodeCreator;
        this.relationshipCreator = relationshipCreator;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getNumberOfNodes() {
        return relationshipGenerator.getNumberOfNodes();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NodeCreator getNodeCreator() {
        return nodeCreator;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public RelationshipCreator getRelationshipCreator() {
        return relationshipCreator;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public RelationshipGenerator getRelationshipGenerator() {
        return relationshipGenerator;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getBatchSize() {
        return 1000;
    }
}
