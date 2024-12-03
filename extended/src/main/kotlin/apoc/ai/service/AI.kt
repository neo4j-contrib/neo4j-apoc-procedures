package apoc.ai.service

import apoc.result.MapResultExtended

interface AI {
    fun entities(data: Any, config: Map<String, Any?>): List<Map<String, Any?>>
    fun sentiment(data: Any, config: Map<String, Any?>): List<MapResultExtended>
    fun keyPhrases(data: Any, config: Map<String, Any?>): List<MapResultExtended>
    fun vision(data: Any, config: Map<String, Any?>): List<MapResultExtended>

}

