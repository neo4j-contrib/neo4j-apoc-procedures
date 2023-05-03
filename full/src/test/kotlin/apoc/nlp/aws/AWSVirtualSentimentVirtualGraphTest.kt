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
package apoc.nlp.aws

import apoc.nlp.NodeMatcher
import apoc.nlp.aws.AWSVirtualSentimentVirtualGraph.Companion.extractSentiment
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
                Pair("Mixed", 0.7F),
                extractSentiment(mapOf(
                        "sentiment" to "MIXED",
                        "sentimentScore" to mapOf("positive" to null, "negative" to null, "neutral" to null, "mixed" to 0.7F))))

        assertEquals(
                Pair("Negative", 0.9F),
                extractSentiment(mapOf(
                        "sentiment" to "NEGATIVE",
                        "sentimentScore" to mapOf("positive" to null, "negative" to 0.9F, "neutral" to null, "mixed" to null))))

        assertEquals(
                Pair("Neutral", 0.6F),
                extractSentiment(mapOf(
                        "sentiment" to "NEUTRAL",
                        "sentimentScore" to mapOf("positive" to null, "negative" to null, "neutral" to 0.6F, "mixed" to null))))

        assertEquals(
                Pair("Positive", 0.4F),
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
        // we cannot assert initial NodeMatcher
        // because the sentiment.graph return a virtual node with other properties (sentimentScore and sentiment)
        assertThat(nodes, hasItem(NodeMatcher(listOf(Label { "Person" }), mapOf("sentiment" to "Mixed", "sentimentScore" to 0.8F, "id" to 1234L))))
    }
}

