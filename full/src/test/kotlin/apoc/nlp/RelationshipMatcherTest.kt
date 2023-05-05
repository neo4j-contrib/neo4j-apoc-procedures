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
import apoc.result.VirtualRelationship
import junit.framework.Assert.assertFalse
import junit.framework.Assert.assertTrue
import org.junit.Test
import org.neo4j.graphdb.Label
import org.neo4j.graphdb.RelationshipType


class RelationshipMatcherTest {
    @Test
    fun `all the same`() {
        val startNode = VirtualNode(arrayOf(Label { "Person" }), mapOf("id" to 1234L))
        val relationshipType = "ENTITY"
        val endNode = VirtualNode(arrayOf(Label { "Organisation" }), mapOf("name" to "Neo4j"))
        val matcher = RelationshipMatcher(startNode, endNode, relationshipType, mapOf("score" to 0.9F))

        val relationship = VirtualRelationship(startNode, endNode, RelationshipType { relationshipType })
        relationship.setProperty("score", 0.9F)

        assertTrue(matcher.matches(relationship))
    }

    @Test
    fun `different relationship type`() {
        val startNode = VirtualNode(arrayOf(Label { "Person" }), mapOf("id" to 1234L))
        val relationshipType = "ENTITY"
        val endNode = VirtualNode(arrayOf(Label { "Organisation" }), mapOf("name" to "Neo4j"))
        val matcher = RelationshipMatcher(startNode, endNode, relationshipType, mapOf("score" to 0.9F))

        val relationship = VirtualRelationship(startNode, endNode, RelationshipType { "BLAH" })
        relationship.setProperty("score", 0.9F)
        assertFalse(matcher.matches(relationship))
    }

    @Test
    fun `different start node`() {
        val startNode = VirtualNode(arrayOf(Label { "Person" }), mapOf("id" to 1234L))
        val relationshipType = "ENTITY"
        val endNode = VirtualNode(arrayOf(Label { "Organisation" }), mapOf("name" to "Neo4j"))
        val matcher = RelationshipMatcher(startNode, endNode, relationshipType, mapOf("score" to 0.9F))

        val actualStartNode = VirtualNode(arrayOf(Label { "Person" }), mapOf("id" to 1235L))
        val relationship = VirtualRelationship(actualStartNode, endNode, RelationshipType { "BLAH" })
        relationship.setProperty("score", 0.9F)
        assertFalse(matcher.matches(relationship))
    }

    @Test
    fun `different end node`() {
        val startNode = VirtualNode(arrayOf(Label { "Person" }), mapOf("id" to 1234L))
        val relationshipType = "ENTITY"
        val endNode = VirtualNode(arrayOf(Label { "Organisation" }), mapOf("name" to "Neo4j"))
        val matcher = RelationshipMatcher(startNode, endNode, relationshipType, mapOf("score" to 0.9F))

        val actualEndNode = VirtualNode(arrayOf(Label { "Human" }), mapOf("id" to 1234L))
        val relationship = VirtualRelationship(startNode, actualEndNode, RelationshipType { "BLAH" })
        relationship.setProperty("score", 0.9F)
        assertFalse(matcher.matches(relationship))
    }

    @Test
    fun `different properties`() {
        val startNode = VirtualNode(arrayOf(Label { "Person" }), mapOf("id" to 1234L))
        val relationshipType = "ENTITY"
        val endNode = VirtualNode(arrayOf(Label { "Organisation" }), mapOf("name" to "Neo4j"))
        val matcher = RelationshipMatcher(startNode, endNode, relationshipType, mapOf("score" to 0.95F))

        val relationship = VirtualRelationship(startNode, endNode, RelationshipType { relationshipType })
        relationship.setProperty("score", 0.9F)

        assertFalse(matcher.matches(relationship))
    }
}

