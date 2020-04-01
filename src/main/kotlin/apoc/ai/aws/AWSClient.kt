package apoc.ai.aws

import apoc.ai.azure.AzureClient
import apoc.ai.dto.AIMapResult
import apoc.ai.service.AI
import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.comprehend.AmazonComprehendClientBuilder
import com.amazonaws.services.comprehend.model.BatchDetectEntitiesRequest
import com.amazonaws.services.comprehend.model.BatchDetectKeyPhrasesRequest
import com.amazonaws.services.comprehend.model.BatchDetectSentimentRequest
import org.neo4j.logging.Log
import java.lang.UnsupportedOperationException

class AWSClient(apiKey: String, apiSecret: String, private val log: Log): AI {

    private val awsClient = AmazonComprehendClientBuilder.standard()
            .withCredentials(AWSStaticCredentialsProvider(BasicAWSCredentials(apiKey, apiSecret)))
            .build()

    override fun entities(data: Any, config: Map<String, Any?>): List<AIMapResult> {
        val convertedData = convertInput(data)
        var batch = BatchDetectEntitiesRequest().withTextList(convertedData)
        var batchDetectEntities = awsClient.batchDetectEntities(batch)

        val allData = batchDetectEntities.resultList
        val batchToRetry = batchDetectEntities.errorList.map { convertedData[it.index] }
        batch = BatchDetectEntitiesRequest().withTextList(batchToRetry)
        batchDetectEntities = awsClient.batchDetectEntities(batch)
        allData += batchDetectEntities.resultList

        return allData.map { AIMapResult(AzureClient.MAPPER.convertValue(it, Map::class.java) as Map<String, Any?>) }
    }

    override fun sentiment(data: Any, config: Map<String, Any?>): List<AIMapResult> {
        val convertedData = convertInput(data)
        var batch = BatchDetectSentimentRequest().withTextList(convertedData)
        var batchDetectEntities = awsClient.batchDetectSentiment(batch)

        val allData = batchDetectEntities.resultList
        val batchToRetry = batchDetectEntities.errorList.map { convertedData[it.index] }
        batch = BatchDetectSentimentRequest().withTextList(batchToRetry)
        batchDetectEntities = awsClient.batchDetectSentiment(batch)
        allData += batchDetectEntities.resultList

        return allData.map { AIMapResult(AzureClient.MAPPER.convertValue(it, Map::class.java) as Map<String, Any?>) }
    }

    override fun keyPhrases(data: Any, config: Map<String, Any?>): List<AIMapResult> {
        val convertedData = convertInput(data)
        var batch = BatchDetectKeyPhrasesRequest().withTextList(convertedData)
        var batchDetectEntities = awsClient.batchDetectKeyPhrases(batch)

        val allData = batchDetectEntities.resultList
        val batchToRetry = batchDetectEntities.errorList.map { convertedData[it.index] }
        batch = BatchDetectKeyPhrasesRequest().withTextList(batchToRetry)
        batchDetectEntities = awsClient.batchDetectKeyPhrases(batch)
        allData += batchDetectEntities.resultList

        return allData.map { AIMapResult(AzureClient.MAPPER.convertValue(it, Map::class.java) as Map<String, Any?>) }
    }

    override fun vision(data: Any, config: Map<String, Any?>): List<AIMapResult> {
        throw UnsupportedOperationException("Rekognition is not yet implemented")
    }

    private fun convertInput(data: Any): List<String> {
        return when (data) {
            is Map<*, *> -> {
                val map = data as Map<String, String>
                val list = arrayOfNulls<String>(map.size)
                map.mapKeys { it.key.toInt() }
                        .forEach { k, v -> list[k] = v }
                list.mapNotNull { it ?: "" }.toList()
            }
            is Collection<*> -> data as List<String>
            is String -> listOf(data)
            else -> throw java.lang.RuntimeException("Class ${data::class.java.name} not supported")
        }
    }
}