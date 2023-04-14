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
 * Unit test for {@link WattsStrogatzConfig}.
 */
public class WattsStrogatzConfigTest {

    @Test
    public void shouldCorrectlyEvaluateValidConfig() {
        assertFalse(new WattsStrogatzConfig(-1, 4, 0.5).isValid());
        assertFalse(new WattsStrogatzConfig(0, 4, 0.5).isValid());
        assertFalse(new WattsStrogatzConfig(1, 4, 0.5).isValid());
        assertFalse(new WattsStrogatzConfig(2, 4, 0.5).isValid());
        assertFalse(new WattsStrogatzConfig(3, 4, 0.5).isValid());
        assertFalse(new WattsStrogatzConfig(4, 4, 0.5).isValid());
        assertTrue(new WattsStrogatzConfig(5, 4, 0.5).isValid());
        assertTrue(new WattsStrogatzConfig(6, 4, 0.5).isValid());

        assertFalse(new WattsStrogatzConfig(6, 3, 0.5).isValid());
        assertFalse(new WattsStrogatzConfig(6, 2, 0.5).isValid());
        assertFalse(new WattsStrogatzConfig(6, 1, 0.5).isValid());
        assertFalse(new WattsStrogatzConfig(6, 0, 0.5).isValid());
        assertFalse(new WattsStrogatzConfig(6, -1, 0.5).isValid());
        assertFalse(new WattsStrogatzConfig(6, 5, 0.5).isValid());
        assertFalse(new WattsStrogatzConfig(6, 6, 0.5).isValid());
        assertFalse(new WattsStrogatzConfig(6, 7, 0.5).isValid());
        assertFalse(new WattsStrogatzConfig(6, 8, 0.5).isValid());
        assertFalse(new WattsStrogatzConfig(6, 111, 0.5).isValid());

        assertFalse(new WattsStrogatzConfig(6, 4, -0.01).isValid());
        assertTrue(new WattsStrogatzConfig(6, 4, 0).isValid());
        assertTrue(new WattsStrogatzConfig(6, 4, 0.01).isValid());
        assertTrue(new WattsStrogatzConfig(6, 4, 0.99).isValid());
        assertTrue(new WattsStrogatzConfig(6, 4, 1.00).isValid());
        assertFalse(new WattsStrogatzConfig(6, 4, 1.01).isValid());
    }
}
