package apoc.nlp

import apoc.result.VirtualNodeExtended
import apoc.result.VirtualRelationshipExtended
import junit.framework.Assert.assertFalse
import junit.framework.Assert.assertTrue
import org.junit.Test
import org.neo4j.graphdb.Label
import org.neo4j.graphdb.RelationshipType


class RelationshipMatcherTest {
    @Test
    fun `all the same`() {
        val startNode =
            VirtualNodeExtended(arrayOf(Label { "Person" }), mapOf("id" to 1234L))
        val relationshipType = "ENTITY"
        val endNode = VirtualNodeExtended(
            arrayOf(Label { "Organisation" }),
            mapOf("name" to "Neo4j")
        )
        val matcher = RelationshipMatcher(startNode, endNode, relationshipType, mapOf("score" to 0.9F))

        val relationship = VirtualRelationshipExtended(
            startNode,
            endNode,
            RelationshipType { relationshipType })
        relationship.setProperty("score", 0.9F)

        assertTrue(matcher.matches(relationship))
    }

    @Test
    fun `different relationship type`() {
        val startNode =
            VirtualNodeExtended(arrayOf(Label { "Person" }), mapOf("id" to 1234L))
        val relationshipType = "ENTITY"
        val endNode = VirtualNodeExtended(
            arrayOf(Label { "Organisation" }),
            mapOf("name" to "Neo4j")
        )
        val matcher = RelationshipMatcher(startNode, endNode, relationshipType, mapOf("score" to 0.9F))

        val relationship =
            VirtualRelationshipExtended(startNode, endNode, RelationshipType { "BLAH" })
        relationship.setProperty("score", 0.9F)
        assertFalse(matcher.matches(relationship))
    }

    @Test
    fun `different start node`() {
        val startNode =
            VirtualNodeExtended(arrayOf(Label { "Person" }), mapOf("id" to 1234L))
        val relationshipType = "ENTITY"
        val endNode = VirtualNodeExtended(
            arrayOf(Label { "Organisation" }),
            mapOf("name" to "Neo4j")
        )
        val matcher = RelationshipMatcher(startNode, endNode, relationshipType, mapOf("score" to 0.9F))

        val actualStartNode =
            VirtualNodeExtended(arrayOf(Label { "Person" }), mapOf("id" to 1235L))
        val relationship = VirtualRelationshipExtended(
            actualStartNode,
            endNode,
            RelationshipType { "BLAH" })
        relationship.setProperty("score", 0.9F)
        assertFalse(matcher.matches(relationship))
    }

    @Test
    fun `different end node`() {
        val startNode =
            VirtualNodeExtended(arrayOf(Label { "Person" }), mapOf("id" to 1234L))
        val relationshipType = "ENTITY"
        val endNode = VirtualNodeExtended(
            arrayOf(Label { "Organisation" }),
            mapOf("name" to "Neo4j")
        )
        val matcher = RelationshipMatcher(startNode, endNode, relationshipType, mapOf("score" to 0.9F))

        val actualEndNode =
            VirtualNodeExtended(arrayOf(Label { "Human" }), mapOf("id" to 1234L))
        val relationship = VirtualRelationshipExtended(
            startNode,
            actualEndNode,
            RelationshipType { "BLAH" })
        relationship.setProperty("score", 0.9F)
        assertFalse(matcher.matches(relationship))
    }

    @Test
    fun `different properties`() {
        val startNode =
            VirtualNodeExtended(arrayOf(Label { "Person" }), mapOf("id" to 1234L))
        val relationshipType = "ENTITY"
        val endNode = VirtualNodeExtended(
            arrayOf(Label { "Organisation" }),
            mapOf("name" to "Neo4j")
        )
        val matcher = RelationshipMatcher(startNode, endNode, relationshipType, mapOf("score" to 0.95F))

        val relationship = VirtualRelationshipExtended(
            startNode,
            endNode,
            RelationshipType { relationshipType })
        relationship.setProperty("score", 0.9F)

        assertFalse(matcher.matches(relationship))
    }
}

