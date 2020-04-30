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
                     Entity().withText("token-1-index-$index-batch-$batchId").withType("Dummy"),
                     Entity().withText("token-2-index-$index-batch-$batchId").withType("Dummy"))
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

    private fun convertInput(data: List<Node>): List<String> {
        return data.map { node -> node.getProperty(nodeProperty).toString() }
    }
}