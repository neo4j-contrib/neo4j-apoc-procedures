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
 * Abstract base-class for {@link RelationshipGenerator} implementations.
 *
 * @param <T> type of accepted configuration.
 */
public abstract class BaseRelationshipGenerator<T extends RelationshipGeneratorConfig> implements RelationshipGenerator {

    private final T configuration;

    /**
     * Construct a new relationship generator.
     *
     * @param configuration to base the generation on
     */
    protected BaseRelationshipGenerator(T configuration) {
        this.configuration = configuration;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getNumberOfNodes() {
        return configuration.getNumberOfNodes();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Pair<Integer, Integer>> generateEdges() throws InvalidConfigException {
        if (!configuration.isValid()) {
            throw new InvalidConfigException("The supplied config is not valid");
        }

        return doGenerateEdges();
    }

    /**
     * Perform the actual edge generation.
     *
     * @return generated edges as pair of node IDs that should be connected.
     */
    protected abstract List<Pair<Integer, Integer>> doGenerateEdges();

    /**
     * Get the configuration of this generator.
     *
     * @return configuration.
     */
    protected T getConfiguration() {
        return configuration;
    }
}
