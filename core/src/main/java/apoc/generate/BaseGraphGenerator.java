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
package apoc.generate;

import apoc.generate.config.GeneratorConfiguration;

import java.util.List;

/**
 * Base class for {@link GraphGenerator} implementations.
 */
public abstract class BaseGraphGenerator implements GraphGenerator {

    /**
     * {@inheritDoc}
     */
    @Override
    public void generateGraph(GeneratorConfiguration configuration) {
        generateRelationships(configuration, generateNodes(configuration));
    }

    /**
     * Generate (i.e. create and persist) nodes.
     *
     * @param configuration generator config.
     * @return list of node IDs of the generated nodes.
     */
    protected abstract List<Long> generateNodes(GeneratorConfiguration configuration);

    /**
     * Generate (i.e. create and persist) relationships.
     *
     * @param config generator config.
     * @param nodes  list of node IDs of the generated nodes.
     */
    protected abstract void generateRelationships(final GeneratorConfiguration config, List<Long> nodes);
}
