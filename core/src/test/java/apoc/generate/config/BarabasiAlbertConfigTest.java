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
 *  Unit test for {@link BarabasiAlbertConfig}.
 */
public class BarabasiAlbertConfigTest {

    @Test
    public void shouldCorrectlyEvaluateValidConfig() {
        assertFalse(new BarabasiAlbertConfig(-1, 2).isValid());
        assertFalse(new BarabasiAlbertConfig(0, 2).isValid());
        assertFalse(new BarabasiAlbertConfig(1, 2).isValid());
        assertFalse(new BarabasiAlbertConfig(2, 2).isValid());
        assertFalse(new BarabasiAlbertConfig(3, 0).isValid());
        assertFalse(new BarabasiAlbertConfig(3, -1).isValid());
        assertFalse(new BarabasiAlbertConfig(3000, 3000).isValid());
        assertTrue(new BarabasiAlbertConfig(3, 1).isValid());
        assertTrue(new BarabasiAlbertConfig(2, 1).isValid());
        assertTrue(new BarabasiAlbertConfig(3000, 2999).isValid());
        assertTrue(new BarabasiAlbertConfig(3, 2).isValid());
        assertTrue(new BarabasiAlbertConfig(4, 2).isValid());
        assertTrue(new BarabasiAlbertConfig(Integer.MAX_VALUE - 1, 2).isValid());
        assertTrue(new BarabasiAlbertConfig(Integer.MAX_VALUE, 2).isValid());
        //noinspection NumericOverflow
        assertFalse(new BarabasiAlbertConfig(Integer.MAX_VALUE + 1, 2).isValid());
    }
}
