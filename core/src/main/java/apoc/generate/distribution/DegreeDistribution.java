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
package apoc.generate.distribution;

import java.util.List;

/**
 * A distribution of node degrees for {@link apoc.generate.relationship.RelationshipGenerator}s.
 */
public interface DegreeDistribution {

    /**
     * @return true if the config is valid.
     */
    boolean isValid();

    /**
     * Get the node degrees produced by this distribution.
     *
     * @return node degrees. Should be immutable (read-only).
     */
    List<Integer> getDegrees();

    /**
     * @return true iff this distribution is a zero-list.
     */
    boolean isZeroList();

    /**
     * Get degree by index.
     *
     * @param index of the degree to get.
     * @return degree.
     */
    int get(int index);

    /**
     * Get the size of the distribution, i.e., the number of nodes.
     *
     * @return size.
     */
    int size();
}
