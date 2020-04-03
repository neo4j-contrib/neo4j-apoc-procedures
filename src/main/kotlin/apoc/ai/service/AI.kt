package apoc.ai.service

import apoc.ai.dto.AIMapResult

interface AI {

    fun entities(data: Any, config: Map<String, Any?>): List<AIMapResult>
    fun sentiment(data: Any, config: Map<String, Any?>): List<AIMapResult>
    fun keyPhrases(data: Any, config: Map<String, Any?>): List<AIMapResult>
    fun vision(data: Any, config: Map<String, Any?>): List<AIMapResult>

}