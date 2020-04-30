package apoc.ai.service

import com.amazonaws.services.comprehend.model.BatchDetectEntitiesResult
import com.amazonaws.services.comprehend.model.BatchDetectKeyPhrasesResult
import org.neo4j.graphdb.Node

interface AWSClient {
    fun entities(data: List<Node>, batchId: Int): BatchDetectEntitiesResult?
    fun keyPhrases(data: List<Node>, batchId: Int): BatchDetectKeyPhrasesResult?
}