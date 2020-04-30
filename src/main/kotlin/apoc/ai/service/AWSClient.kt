package apoc.ai.service

import com.amazonaws.services.comprehend.model.BatchDetectEntitiesResult
import org.neo4j.graphdb.Node

interface AWSClient {
    fun entities(data: List<Node>): BatchDetectEntitiesResult?
}