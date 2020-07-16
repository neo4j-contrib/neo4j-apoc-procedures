package apoc.nlp.azure

import apoc.nlp.NodeMatcher
import apoc.result.VirtualNode
import com.amazonaws.services.comprehend.model.*
import junit.framework.Assert.assertEquals
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.hasItem
import org.junit.ClassRule
import org.junit.Test
import org.neo4j.graphdb.Label
import org.neo4j.test.rule.ImpermanentDbmsRule

class AzureVirtualSentimentGraphStoreTest {

    companion object {
        @ClassRule
        @JvmField
        val neo4j = ImpermanentDbmsRule()
    }

    @Test
    fun `create virtual graph from result with one entity`() {
        neo4j.beginTx().use {
            val sourceNode = VirtualNode(arrayOf(Label { "Person" }), mapOf("id" to 1234L))

            val res = listOf(
                    mapOf("id" to sourceNode.id.toString(), "score" to 0.75)
            )

            val virtualGraph = AzureVirtualSentimentVirtualGraph(res, listOf(sourceNode)).createAndStore(it)

            val nodes = virtualGraph.graph["nodes"] as Set<*>
            assertEquals(1, nodes.size)
            assertThat(nodes, hasItem(sourceNode))
            assertThat(nodes, hasItem(NodeMatcher(listOf(Label { "Person" }), mapOf( "sentimentScore" to 0.75, "id" to 1234L))))
        }
    }
}

