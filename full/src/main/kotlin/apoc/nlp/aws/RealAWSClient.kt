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

import apoc.result.MapResult
import apoc.util.JsonUtil
import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.comprehend.AmazonComprehendClientBuilder
import com.amazonaws.services.comprehend.model.BatchDetectEntitiesRequest
import com.amazonaws.services.comprehend.model.BatchDetectEntitiesResult
import com.amazonaws.services.comprehend.model.BatchDetectKeyPhrasesRequest
import com.amazonaws.services.comprehend.model.BatchDetectKeyPhrasesResult
import com.amazonaws.services.comprehend.model.BatchDetectSentimentRequest
import com.amazonaws.services.comprehend.model.BatchDetectSentimentResult
import org.neo4j.graphdb.Node
import org.neo4j.logging.Log

class RealAWSClient(config: Map<String, Any>, private val log: Log) : AWSClient {
    private val apiKey = config["key"].toString()
    private val apiSecret = config["secret"].toString()
    private val region = config.getOrDefault("region", "us-east-1").toString()
    private val language = config.getOrDefault("language", "en").toString()
    private val nodeProperty = config.getOrDefault("nodeProperty", "text").toString()

    private val awsClient = AmazonComprehendClientBuilder.standard()
            .withCredentials(AWSStaticCredentialsProvider(BasicAWSCredentials(apiKey, apiSecret)))
            .withRegion(region)
            .build()

     override fun entities(data: List<Node>, batchId: Int): BatchDetectEntitiesResult? {
         val convertedData = convertInput(data)
         val batch = BatchDetectEntitiesRequest().withTextList(convertedData).withLanguageCode(language)
         return awsClient.batchDetectEntities(batch)
    }

    override fun keyPhrases(data: List<Node>, batchId: Int): BatchDetectKeyPhrasesResult? {
        val convertedData = convertInput(data)
        val batch = BatchDetectKeyPhrasesRequest().withTextList(convertedData).withLanguageCode(language)
        return awsClient.batchDetectKeyPhrases(batch)
    }

    override fun sentiment(data: List<Node>, batchId: Int): BatchDetectSentimentResult? {
        val convertedData = convertInput(data)
        val batch = BatchDetectSentimentRequest().withTextList(convertedData).withLanguageCode(language)
        return awsClient.batchDetectSentiment(batch)
    }

    fun sentiment(data: List<Node>, config: Map<String, Any?>): List<MapResult> {
        val convertedData = convertInput(data)
        var batch = BatchDetectSentimentRequest().withTextList(convertedData)
        var batchDetectEntities = awsClient.batchDetectSentiment(batch)

        val allData = batchDetectEntities.resultList
        val batchToRetry = batchDetectEntities.errorList.map { convertedData[it.index] }
        batch = BatchDetectSentimentRequest().withTextList(batchToRetry)
        batchDetectEntities = awsClient.batchDetectSentiment(batch)
        allData += batchDetectEntities.resultList

        return allData.map { MapResult(JsonUtil.OBJECT_MAPPER!!.convertValue(it, Map::class.java) as Map<String, Any?>) }
    }

     fun keyPhrases(data: List<Node>, config: Map<String, Any?>): List<MapResult> {
        val convertedData = convertInput(data)
        var batch = BatchDetectKeyPhrasesRequest().withTextList(convertedData)
        var batchDetectEntities = awsClient.batchDetectKeyPhrases(batch)

        val allData = batchDetectEntities.resultList
        val batchToRetry = batchDetectEntities.errorList.map { convertedData[it.index] }
        batch = BatchDetectKeyPhrasesRequest().withTextList(batchToRetry)
        batchDetectEntities = awsClient.batchDetectKeyPhrases(batch)
        allData += batchDetectEntities.resultList

        return allData.map { MapResult(JsonUtil.OBJECT_MAPPER!!.convertValue(it, Map::class.java) as Map<String, Any?>) }
    }

     fun vision(data: Any, config: Map<String, Any?>): List<MapResult> {
        throw UnsupportedOperationException("Rekognition is not yet implemented")
    }

    private fun convertInput(data: List<Node>): List<String> {
        return data.map { node -> node.getProperty(nodeProperty).toString() }
    }
}