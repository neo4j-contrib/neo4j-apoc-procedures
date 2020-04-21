package apoc.ai.service

import apoc.result.MapResult

interface AI {
    fun entities(data: Any, config: Map<String, Any?>): List<Map<String, Any?>>
    fun sentiment(data: Any, config: Map<String, Any?>): List<MapResult>
    fun keyPhrases(data: Any, config: Map<String, Any?>): List<MapResult>
    fun vision(data: Any, config: Map<String, Any?>): List<MapResult>

}

