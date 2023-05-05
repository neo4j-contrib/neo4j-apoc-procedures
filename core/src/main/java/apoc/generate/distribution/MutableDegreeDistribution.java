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

import java.util.Comparator;

/**
 * A mutable {@link DegreeDistribution}.
 */
public interface MutableDegreeDistribution extends DegreeDistribution {

    /**
     * Set degree by index.
     *
     * @param index of the degree to set.
     * @param value to set.
     */
    void set(int index, int value);

    /**
     * Decrease the degree at the specified index by 1.
     *
     * @param index to decrease.
     */
    void decrease(int index);

    /**
     * Sort the degree distribution using the given comparator.
     *
     * @param comparator comparator.
     */
    void sort(Comparator<Integer> comparator);
}
