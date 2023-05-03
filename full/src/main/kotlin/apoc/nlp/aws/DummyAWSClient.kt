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

import com.amazonaws.services.comprehend.model.*
import org.neo4j.graphdb.Node
import org.neo4j.logging.Log

class DummyAWSClient(config: Map<String, Any>, private val log: Log) : AWSClient {
    private val nodeProperty = config.getOrDefault("nodeProperty", "text").toString()

     override fun entities(data: List<Node>, batchId: Int): BatchDetectEntitiesResult? {
         val convertedData = convertInput(data)

         val batchResults: MutableList<BatchDetectEntitiesItemResult> = mutableListOf()

         convertedData.mapIndexed { index, _ ->
             batchResults += BatchDetectEntitiesItemResult().withEntities(
                     Entity().withText("token-1-index-$index-batch-$batchId").withType(EntityType.COMMERCIAL_ITEM).withScore(0.5F),
                     Entity().withText("token-2-index-$index-batch-$batchId").withType(EntityType.ORGANIZATION).withScore(0.7F))
                     .withIndex(index)
         }

         return BatchDetectEntitiesResult().withErrorList(BatchItemError()).withResultList(batchResults)
    }

    override fun keyPhrases(data: List<Node>, batchId: Int): BatchDetectKeyPhrasesResult? {
        val convertedData = convertInput(data)

        val batchResults: MutableList<BatchDetectKeyPhrasesItemResult> = mutableListOf()

        convertedData.mapIndexed { index, value ->
            batchResults += BatchDetectKeyPhrasesItemResult().withKeyPhrases(
                    KeyPhrase().withText("keyPhrase-1-index-$index-batch-$batchId").withScore(0.3F),
                    KeyPhrase().withText("keyPhrase-2-index-$index-batch-$batchId").withScore(0.4F))
                    .withIndex(index)
        }

        return BatchDetectKeyPhrasesResult().withErrorList(BatchItemError()).withResultList(batchResults)
    }

    override fun sentiment(data: List<Node>, batchId: Int): BatchDetectSentimentResult? {
        val convertedData = convertInput(data)

        val batchResults: MutableList<BatchDetectSentimentItemResult> = mutableListOf()

        convertedData.mapIndexed { index, value ->
            batchResults += BatchDetectSentimentItemResult()
                    .withSentiment(SentimentType.MIXED)
                    .withSentimentScore(SentimentScore().withMixed(0.7F))
                    .withIndex(index)
        }

        return BatchDetectSentimentResult().withErrorList(BatchItemError()).withResultList(batchResults)
    }

    private fun convertInput(data: List<Node>): List<String> {
        return data.map { node -> node.getProperty(nodeProperty).toString() }
    }
}
