package apoc.nlp.azure

interface AzureClient {
    fun entities(data: List<List<Map<String, Any>>>): List<Map<String, Any>>

    fun sentiment(data: List<List<Map<String, Any>>>): List<Map<String, Any>>

    fun keyPhrases(data: List<List<Map<String, Any>>>): List<Map<String, Any>>
}
