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

import apoc.generate.distribution.SimpleDegreeDistribution;

import java.util.List;

/**
 * {@link SimpleDegreeDistribution} serving as a {@link RelationshipGeneratorConfig}.
 *
 * The sum of all degrees should be an even integer. Moreover, not all distributions
 * correspond to a simple, undirected graph. Only graphs that satisfy Erdos-Gallai condition
 * or equivalently the Havel-Hakimi test are valid. The distribution is tested in {@link SimpleDegreeDistribution} class.
 */
public class DistributionBasedConfig extends SimpleDegreeDistribution implements RelationshipGeneratorConfig {

    /**
     * Create a new config.
     *
     * @param degrees list of node degrees.
     */
    public DistributionBasedConfig(List<Integer> degrees) {
        super(degrees);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getNumberOfNodes() {
        return size();
    }
}
