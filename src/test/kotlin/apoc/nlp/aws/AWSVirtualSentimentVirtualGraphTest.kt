package apoc.nlp.aws

import apoc.nlp.AWSVirtualSentimentVirtualGraph
import apoc.nlp.AWSVirtualSentimentVirtualGraph.Companion.extractSentiment
import apoc.result.VirtualNode
import com.amazonaws.services.comprehend.model.*
import junit.framework.Assert.assertEquals
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.hasItem
import org.junit.Test
import org.neo4j.graphdb.Label

class AWSVirtualSentimentVirtualGraphTest {
    @Test
    fun `extract sentiment`() {
        assertEquals(
                mapOf("sentiment" to "Mixed", "score" to 0.7F),
                extractSentiment(mapOf(
                        "sentiment" to "MIXED",
                        "sentimentScore" to mapOf("positive" to null, "negative" to null, "neutral" to null, "mixed" to 0.7F))))

        assertEquals(
                mapOf("sentiment" to "Negative", "score" to 0.9F),
                extractSentiment(mapOf(
                        "sentiment" to "NEGATIVE",
                        "sentimentScore" to mapOf("positive" to null, "negative" to 0.9F, "neutral" to null, "mixed" to null))))

        assertEquals(
                mapOf("sentiment" to "Neutral", "score" to 0.6F),
                extractSentiment(mapOf(
                        "sentiment" to "NEUTRAL",
                        "sentimentScore" to mapOf("positive" to null, "negative" to null, "neutral" to 0.6F, "mixed" to null))))

        assertEquals(
                mapOf("sentiment" to "Positive", "score" to 0.4F),
                extractSentiment(mapOf(
                        "sentiment" to "POSITIVE",
                        "sentimentScore" to mapOf("positive" to 0.4F, "negative" to null, "neutral" to null, "mixed" to null))))
    }

    @Test
    fun `create virtual graph from result with one entity`() {
        val itemResult = BatchDetectSentimentItemResult().withSentiment(SentimentType.MIXED).withSentimentScore(SentimentScore().withMixed(0.8F)).withIndex(0)
        val res = BatchDetectSentimentResult().withErrorList(BatchItemError()).withResultList(itemResult)
        val sourceNode = VirtualNode(arrayOf(Label {"Person"}), mapOf("id" to 1234L))

        val virtualGraph = AWSVirtualSentimentVirtualGraph(res, listOf(sourceNode)).create()

        val nodes = virtualGraph.graph["nodes"] as Set<*>
        assertEquals(1, nodes.size)
        assertThat(nodes, hasItem(sourceNode))
        assertThat(nodes, hasItem(NodeMatcher(listOf(Label { "Person" }), mapOf("sentiment" to "Mixed", "score" to 0.8F, "id" to 1234L))))
    }
}

