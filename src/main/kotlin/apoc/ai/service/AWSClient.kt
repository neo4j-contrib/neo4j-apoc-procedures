package apoc.ai.service

import com.amazonaws.services.comprehend.model.BatchDetectEntitiesResult

interface AWSClient {
    fun entities(data: Any): BatchDetectEntitiesResult?
}