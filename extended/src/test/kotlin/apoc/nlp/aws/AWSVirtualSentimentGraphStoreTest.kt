package apoc.nlp.aws

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

class AWSVirtualSentimentGraphStoreTest {

    companion object {
        @ClassRule
        @JvmField
        val neo4j = ImpermanentDbmsRule()
    }

    @Test
    fun `create virtual graph from result with one entity`() {
        neo4j.beginTx().use {
            val itemResult = BatchDetectSentimentItemResult().withSentiment(SentimentType.MIXED).withSentimentScore(SentimentScore().withMixed(0.8F)).withIndex(0)
            val res = BatchDetectSentimentResult().withErrorList(BatchItemError()).withResultList(itemResult)
            val sourceNode = VirtualNode(arrayOf(Label { "Person" }), mapOf("id" to 1234L))

            val virtualGraph = AWSVirtualSentimentVirtualGraph(res, listOf(sourceNode)).createAndStore(it)

            val nodes = virtualGraph.graph["nodes"] as Set<*>
            assertEquals(1, nodes.size)
            assertThat(nodes, hasItem(sourceNode))
            assertThat(nodes, hasItem(NodeMatcher(listOf(Label { "Person" }), mapOf("sentiment" to "Mixed", "sentimentScore" to 0.8F, "id" to 1234L))))
        }
    }
}

