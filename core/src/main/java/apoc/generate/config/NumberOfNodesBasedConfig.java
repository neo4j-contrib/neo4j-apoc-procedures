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
 * {@link RelationshipGeneratorConfig} that is based on an explicitly defined number of nodes in the network.
 */
public class NumberOfNodesBasedConfig implements RelationshipGeneratorConfig {

    private final int numberOfNodes;

    /**
     * Construct a new config.
     *
     * @param numberOfNodes number of nodes present in the network.
     */
    public NumberOfNodesBasedConfig(int numberOfNodes) {
        this.numberOfNodes = numberOfNodes;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getNumberOfNodes() {
        return numberOfNodes;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isValid() {
        return numberOfNodes >= 2;
    }
}
