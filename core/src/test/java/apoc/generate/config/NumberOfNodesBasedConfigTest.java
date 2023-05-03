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

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class NumberOfNodesBasedConfigTest {
    @Test
    public void shouldCorrectlyEvaluateValidConfig() {
        assertFalse(new NumberOfNodesBasedConfig(-1).isValid());
        assertFalse(new NumberOfNodesBasedConfig(0).isValid());
        assertFalse(new NumberOfNodesBasedConfig(1).isValid());

        assertTrue(new NumberOfNodesBasedConfig(3).isValid());
        assertTrue(new NumberOfNodesBasedConfig(5).isValid());

        assertTrue(new NumberOfNodesBasedConfig(Integer.MAX_VALUE - 1).isValid());
        assertTrue(new NumberOfNodesBasedConfig(Integer.MAX_VALUE).isValid());
        //noinspection NumericOverflow
        assertFalse(new NumberOfNodesBasedConfig(Integer.MAX_VALUE + 1).isValid());
    }
}
