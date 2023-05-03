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

import apoc.result.VirtualNode
import junit.framework.Assert.assertFalse
import junit.framework.Assert.assertTrue
import org.hamcrest.StringDescription
import org.junit.Test
import org.neo4j.graphdb.Label


class NodeMatcherTest {
    @Test
    fun `different labels`() {
        val labels = listOf(Label { "Person" })
        val properties = mapOf("id" to 1234L)
        val matcher = NodeMatcher(labels, properties)

        assertFalse(matcher.matches(VirtualNode(arrayOf(Label { "Human" }), properties)))
    }

    @Test
    fun `different properties`() {
        val labels = listOf(Label { "Person" })
        val properties = mapOf("id" to 1234L)
        val matcher = NodeMatcher(labels, properties)

        assertFalse(matcher.matches(VirtualNode(labels.toTypedArray(), mapOf("id" to 5678L))))
    }

    @Test
    fun `different labels and properties`() {
        val labels = listOf(Label { "Person" })
        val properties = mapOf("id" to 1234L)
        val matcher = NodeMatcher(labels, properties)

        assertFalse(matcher.matches(VirtualNode(arrayOf(Label { "Human" }), mapOf("id" to 5678L))))
    }

    @Test
    fun `same labels and properties`() {
        val labels = listOf(Label { "Person" })
        val properties = mapOf("id" to 1234L)
        val matcher = NodeMatcher(labels, properties)

        assertTrue(matcher.matches(VirtualNode(labels.toTypedArray(), properties)))
    }

    @Test
    fun `no labels in actual`() {
        val labels = listOf(Label { "Person" })
        val properties = mapOf("id" to 1234L)
        val matcher = NodeMatcher(listOf(), properties)

        assertFalse(matcher.matches(VirtualNode(labels.toTypedArray(), properties)))
    }

    @Test
    fun `no labels in expected`() {
        val labels = listOf(Label { "Person" })
        val properties = mapOf("id" to 1234L)
        val matcher = NodeMatcher(labels, properties)

        assertFalse(matcher.matches(VirtualNode(arrayOf(), properties)))
    }

    @Test
    fun `no labels in actual and expected`() {
        val properties = mapOf("id" to 1234L)
        val matcher = NodeMatcher(listOf(), properties)

        val description = StringDescription()
        matcher.describeTo(description)
        assertTrue(matcher.matches(VirtualNode(arrayOf(), properties)))
    }

    @Test
    fun `no properties in actual`() {
        val labels = listOf(Label { "Person" })
        val properties = mapOf("id" to 1234L)
        val matcher = NodeMatcher(labels, mapOf())

        assertFalse(matcher.matches(VirtualNode(labels.toTypedArray(), properties)))
    }

    @Test
    fun `no properties in expected`() {
        val labels = listOf(Label { "Person" })
        val properties = mapOf("id" to 1234L)
        val matcher = NodeMatcher(labels, properties)

        assertFalse(matcher.matches(VirtualNode(labels.toTypedArray(), mapOf())))
    }

    @Test
    fun `no properties in expected and actual`() {
        val labels = listOf(Label { "Person" })
        val matcher = NodeMatcher(labels, mapOf())

        assertTrue(matcher.matches(VirtualNode(labels.toTypedArray(), mapOf())))
    }

}

