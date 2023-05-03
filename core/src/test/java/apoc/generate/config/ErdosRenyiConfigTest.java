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

/**
 * Unit test for {@link ErdosRenyiConfig}.
 */
public class ErdosRenyiConfigTest {

    @Test
    public void shouldCorrectlyEvaluateValidConfig() {
        assertFalse(new ErdosRenyiConfig(-1, 5).isValid());
        assertFalse(new ErdosRenyiConfig(0, 5).isValid());
        assertFalse(new ErdosRenyiConfig(1, 5).isValid());
        assertFalse(new ErdosRenyiConfig(2, 5).isValid());
        assertFalse(new ErdosRenyiConfig(3, 5).isValid());
        assertTrue(new ErdosRenyiConfig(4, 5).isValid());
        assertTrue(new ErdosRenyiConfig(5, 5).isValid());
        assertTrue(new ErdosRenyiConfig(10000, 5).isValid());

        assertFalse(new ErdosRenyiConfig(10000, -1).isValid());
        assertFalse(new ErdosRenyiConfig(10000, 0).isValid());
        assertTrue(new ErdosRenyiConfig(10000, 1).isValid());

        assertTrue(new ErdosRenyiConfig(10_000, 5_000 * (10_000 - 1)).isValid());
        assertFalse(new ErdosRenyiConfig(10_000, 5_000 * (10_000 - 1) + 1).isValid());
        assertFalse(new ErdosRenyiConfig(50_000, 25_000 * (50_000 - 1) + 1).isValid());

        assertTrue(new ErdosRenyiConfig(10_000_000, 500_000_000).isValid());
        assertTrue(new ErdosRenyiConfig(100_000_000, 500_000_000).isValid());
        assertTrue(new ErdosRenyiConfig(1_000_000_000, 2_000_000_000).isValid());
        assertTrue(new ErdosRenyiConfig(50_000, 1_249_974_999).isValid());
        assertTrue(new ErdosRenyiConfig(50_000, 1_249_975_000).isValid());
        assertFalse(new ErdosRenyiConfig(50_000, 1_249_975_001).isValid());
        assertFalse(new ErdosRenyiConfig(50_000, 1_249_975_002).isValid());
    }
}
