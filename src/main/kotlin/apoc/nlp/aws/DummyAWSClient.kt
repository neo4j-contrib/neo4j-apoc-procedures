package apoc.nlp.aws

import apoc.ai.service.AWSClient
import com.amazonaws.services.comprehend.model.*
import org.neo4j.graphdb.Node
import org.neo4j.logging.Log

class DummyAWSClient(config: Map<String, Any>, private val log: Log) : AWSClient {
    private val nodeProperty = config.getOrDefault("nodeProperty", "text").toString()

     override fun entities(data: List<Node>, batchId: Int): BatchDetectEntitiesResult? {
         val convertedData = convertInput(data)

         val batchResults: MutableList<BatchDetectEntitiesItemResult> = mutableListOf()

         convertedData.mapIndexed { index, value ->
             batchResults += BatchDetectEntitiesItemResult().withEntities(
                     Entity().withText("token-1-index-$index-batch-$batchId").withType(EntityType.COMMERCIAL_ITEM),
                     Entity().withText("token-2-index-$index-batch-$batchId").withType(EntityType.ORGANIZATION))
                     .withIndex(index)
         }

         return BatchDetectEntitiesResult().withErrorList(BatchItemError()).withResultList(batchResults)
    }

    override fun keyPhrases(data: List<Node>, batchId: Int): BatchDetectKeyPhrasesResult? {
        val convertedData = convertInput(data)

        val batchResults: MutableList<BatchDetectKeyPhrasesItemResult> = mutableListOf()

        convertedData.mapIndexed { index, value ->
            batchResults += BatchDetectKeyPhrasesItemResult().withKeyPhrases(
                    KeyPhrase().withText("keyPhrase-1-index-$index-batch-$batchId"),
                    KeyPhrase().withText("keyPhrase-2-index-$index-batch-$batchId"))
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
