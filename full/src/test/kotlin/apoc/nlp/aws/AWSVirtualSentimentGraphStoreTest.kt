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

