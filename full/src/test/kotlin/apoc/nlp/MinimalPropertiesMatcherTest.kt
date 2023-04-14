/**
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
package apoc.nlp

import org.junit.Test
import org.junit.jupiter.api.Assertions.*

class MinimalPropertiesMatcherTest {
    @Test
    fun `exact match`() {
        val expected = mapOf("id" to 1234L)
        assertTrue(MinimalPropertiesMatcher(expected).matches(expected))
    }

    @Test
    fun `at least match`() {
        val expected = mapOf("id" to 1234L)
        assertTrue(MinimalPropertiesMatcher(expected).matches(mapOf("id" to 1234L, "name" to "Michael")))
    }

    @Test
    fun `missing items`() {
        val expected = mapOf("id" to 1234L, "name" to "Michael")
        assertFalse(MinimalPropertiesMatcher(expected).matches(mapOf("id" to 1234L)))
    }

    @Test
    fun `different value`() {
        val expected = mapOf("id" to 1234L, "name" to "Michael")
        assertFalse(MinimalPropertiesMatcher(expected).matches(mapOf("id" to 1234L, "name" to "Mark")))
    }
}