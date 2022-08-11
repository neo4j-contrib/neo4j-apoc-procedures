package apoc.nlp.aws

import com.amazonaws.services.comprehend.model.BatchDetectEntitiesResult
import com.amazonaws.services.comprehend.model.BatchDetectKeyPhrasesResult
import com.amazonaws.services.comprehend.model.BatchDetectSentimentResult
import org.neo4j.graphdb.Node

interface AWSClient {
    fun entities(data: List<Node>, batchId: Int): BatchDetectEntitiesResult?
    fun keyPhrases(data: List<Node>, batchId: Int): BatchDetectKeyPhrasesResult?
    fun sentiment(data: List<Node>, batchId: Int): BatchDetectSentimentResult?
}