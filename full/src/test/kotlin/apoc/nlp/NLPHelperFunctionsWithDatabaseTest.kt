package apoc.nlp

import apoc.result.VirtualNode
import org.hamcrest.CoreMatchers.hasItem
import org.hamcrest.MatcherAssert
import org.junit.ClassRule
import org.junit.Test
import org.neo4j.configuration.SettingImpl
import org.neo4j.configuration.SettingValueParser
import org.neo4j.configuration.SettingValueParsers.BOOL
import org.neo4j.graphdb.Label
import org.neo4j.graphdb.RelationshipType
import org.neo4j.test.rule.ImpermanentDbmsRule

class NLPHelperFunctionsWithDatabaseTest {
    companion object {
        @ClassRule
        @JvmField
        val neo4j = ImpermanentDbmsRule()
                .withSetting(SettingImpl.newBuilder( "unsupported.dbms.debug.track_cursor_close", BOOL, false ).build(), false)
                .withSetting(SettingImpl.newBuilder( "unsupported.dbms.debug.trace_cursors", BOOL, false ).build(), false)
    }

    @Test
    fun `virtual create relationship`() {
        neo4j.beginTx().use {
            val sourceNode = VirtualNode(arrayOf(Label { "Source" }), mapOf("name" to "Mark"))
            val targetNode = VirtualNode(arrayOf(Label { "Target" }), mapOf("name" to "Michael"))
            val targetNodeAndScore = Pair(targetNode, 0.75)

            val rel = NLPHelperFunctions.mergeRelationship(sourceNode, targetNodeAndScore, RelationshipType { "ENTITY" }, "score")

            MatcherAssert.assertThat(rel, RelationshipMatcher(sourceNode, targetNode, "ENTITY", mapOf("score" to 0.75)))
        }

    }

    @Test
    fun `virtual use highest score`() {
        neo4j.beginTx().use {
            val sourceNode = VirtualNode(arrayOf(Label {"Source"}), mapOf("name" to "Mark"))
            val targetNode = VirtualNode(arrayOf(Label {"Target"}), mapOf("name" to "Michael"))

            NLPHelperFunctions.mergeRelationship(sourceNode, Pair(targetNode, 0.75), RelationshipType { "ENTITY" }, "score")
            val rel = NLPHelperFunctions.mergeRelationship(sourceNode, Pair(targetNode, 0.72), RelationshipType { "ENTITY" }, "score")

            MatcherAssert.assertThat(rel, RelationshipMatcher(sourceNode, targetNode, "ENTITY", mapOf("score" to 0.75)))
        }
    }

    @Test
    fun `actual use highest score`() {
        neo4j.beginTx().use {
            val sourceNode = it.createNode(Label {"Source"})
            sourceNode.setProperty("name", "Mark")

            val targetNode = it.createNode(Label {"Target"})
            targetNode.setProperty("name", "Michael")

            NLPHelperFunctions.mergeRelationship(sourceNode, Pair(targetNode, 0.75), RelationshipType { "ENTITY" }, "score")


            val rels = listOf(NLPHelperFunctions.mergeRelationship(sourceNode, Pair(targetNode, 0.72), RelationshipType { "ENTITY" }, "score"))

            MatcherAssert.assertThat(rels, hasItem(RelationshipMatcher(sourceNode, targetNode, "ENTITY", mapOf("score" to 0.75))))
        }
    }
}
