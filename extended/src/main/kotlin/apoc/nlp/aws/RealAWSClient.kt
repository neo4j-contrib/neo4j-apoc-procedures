package apoc.nlp.aws

import apoc.result.MapResultExtended
import apoc.util.JsonUtilExtended
import com.amazonaws.auth.*
import com.amazonaws.services.comprehend.AmazonComprehendClientBuilder
import com.amazonaws.services.comprehend.model.*
import org.neo4j.graphdb.Node
import org.neo4j.logging.Log

class RealAWSClient(config: Map<String, Any>, private val log: Log) : AWSClient {
    companion object  {
        val missingCredentialError = """
                Error during AWS credentials retrieving.
                Make sure the key ID and the Secret Key are defined via `key` and `secret` parameters 
                or via one of these ways: https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/DefaultAWSCredentialsProviderChain.html:
                """
    }
    private val apiKey = config["key"]?.toString()
    private val apiSecret = config["secret"]?.toString()
    private val apiSessionToken = config["token"].toString()
    private val region = config.getOrDefault("region", "us-east-1").toString()
    private val language = config.getOrDefault("language", "en").toString()
    private val nodeProperty = config.getOrDefault("nodeProperty", "text").toString()

    private val awsClient = AmazonComprehendClientBuilder.standard()
            .withCredentials(awsStaticCredentialsProvider())
            .withRegion(region)
            .build()

    private fun awsStaticCredentialsProvider(): AWSCredentialsProvider {
        return if (!apiKey.isNullOrEmpty() && !apiSecret.isNullOrEmpty()) {
            AWSStaticCredentialsProvider(getAwsBasicCredentials())
        } else {
            DefaultAWSCredentialsProviderChain()
        }
    }

    private fun getAwsBasicCredentials() = if (apiSessionToken.isEmpty()) { 
        BasicAWSCredentials(apiKey, apiSecret) 
    } else { 
        BasicSessionCredentials(apiKey, apiSecret, apiSessionToken) 
    }


    override fun entities(data: List<Node>, batchId: Int): BatchDetectEntitiesResult? {
        try {
             val convertedData = convertInput(data)
             val batch = BatchDetectEntitiesRequest().withTextList(convertedData).withLanguageCode(language)
             return awsClient.batchDetectEntities(batch)
        } catch (e: Exception) {
            throw RuntimeException(missingCredentialError + e)
        }
    }

    override fun keyPhrases(data: List<Node>, batchId: Int): BatchDetectKeyPhrasesResult? {
        try {
            val convertedData = convertInput(data)
            val batch = BatchDetectKeyPhrasesRequest().withTextList(convertedData).withLanguageCode(language)
            return awsClient.batchDetectKeyPhrases(batch)
        } catch (e: Exception) {
            throw RuntimeException(missingCredentialError + e)
        }
    }

    override fun sentiment(data: List<Node>, batchId: Int): BatchDetectSentimentResult? {
        try {
            val convertedData = convertInput(data)
            val batch = BatchDetectSentimentRequest().withTextList(convertedData).withLanguageCode(language)
            return awsClient.batchDetectSentiment(batch)
        } catch (e: Exception) {
            throw RuntimeException(missingCredentialError + e)
        }
    }

    fun sentiment(data: List<Node>, config: Map<String, Any?>): List<MapResultExtended> {
        val convertedData = convertInput(data)
        var batch = BatchDetectSentimentRequest().withTextList(convertedData)
        var batchDetectEntities = awsClient.batchDetectSentiment(batch)

        val allData = batchDetectEntities.resultList
        val batchToRetry = batchDetectEntities.errorList.map { convertedData[it.index] }
        batch = BatchDetectSentimentRequest().withTextList(batchToRetry)
        batchDetectEntities = awsClient.batchDetectSentiment(batch)
        allData += batchDetectEntities.resultList

        return allData.map {
            MapResultExtended(
                JsonUtilExtended.OBJECT_MAPPER!!.convertValue(
                    it,
                    Map::class.java
                ) as Map<String, Any?>
            )
        }
    }

     fun keyPhrases(data: List<Node>, config: Map<String, Any?>): List<MapResultExtended> {
        val convertedData = convertInput(data)
        var batch = BatchDetectKeyPhrasesRequest().withTextList(convertedData)
        var batchDetectEntities = awsClient.batchDetectKeyPhrases(batch)

        val allData = batchDetectEntities.resultList
        val batchToRetry = batchDetectEntities.errorList.map { convertedData[it.index] }
        batch = BatchDetectKeyPhrasesRequest().withTextList(batchToRetry)
        batchDetectEntities = awsClient.batchDetectKeyPhrases(batch)
        allData += batchDetectEntities.resultList

        return allData.map {
            MapResultExtended(
                JsonUtilExtended.OBJECT_MAPPER!!.convertValue(
                    it,
                    Map::class.java
                ) as Map<String, Any?>
            )
        }
    }

     fun vision(data: Any, config: Map<String, Any?>): List<MapResultExtended> {
        throw UnsupportedOperationException("Rekognition is not yet implemented")
    }

    private fun convertInput(data: List<Node>): List<String> {
        return data.map { node -> node.getProperty(nodeProperty).toString() }
    }
}