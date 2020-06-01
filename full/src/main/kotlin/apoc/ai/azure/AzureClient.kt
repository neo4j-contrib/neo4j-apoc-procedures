package apoc.ai.azure

import apoc.ai.service.AI
import apoc.result.MapResult
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.neo4j.logging.Log
import java.io.DataOutputStream
import java.net.URL
import javax.net.ssl.HttpsURLConnection


enum class AzureEndpoint(val method: String) {
    SENTIMENT("/text/analytics/v2.1/sentiment"),
    KEY_PHRASES("/text/analytics/v2.1/keyPhrases"),
    VISION("/vision/v2.1/analyze"),
    ENTITIES("/text/analytics/v2.1/entities")
}

class AzureClient(private val baseUrl: String, private val key: String, private val log: Log): AI {

    companion object {
        @JvmStatic val MAPPER = jacksonObjectMapper()
    }

    private fun postData(method: String, subscriptionKeyValue: String, data: Any, config: Map<String, Any?> = emptyMap()): List<Map<String, Any?>> {
        val fullUrl = baseUrl + method + config.map { "${it.key}=${it.value}" }
                .joinToString("&")
                .also { if (it.isNullOrBlank()) it else "?$it" }
        val url = URL(fullUrl)
        return postData(url, subscriptionKeyValue, data)
    }

    private fun postData(url: URL, subscriptionKeyValue: String, data: Any): List<Map<String, Any?>> {
        val connection = url.openConnection() as HttpsURLConnection
        connection.requestMethod = "POST"
        connection.setRequestProperty("Ocp-Apim-Subscription-Key", subscriptionKeyValue)
        connection.doOutput = true
        when (data) {
            is ByteArray -> {
                connection.setRequestProperty("Content-Type", "application/octet-stream")
                DataOutputStream(connection.outputStream).use { it.write(data) }
            }
            else -> {
                connection.setRequestProperty("Content-Type", "text/json")
                DataOutputStream(connection.outputStream).use { it.write(MAPPER.writeValueAsBytes(mapOf("documents" to convertInput(data)))) }
            }
        }
        return connection.inputStream
                .use { MAPPER.readValue(it, Any::class.java) }
                .let {result ->
                    val documents = (result as Map<String, Any?>)["documents"] as List<Map<String, Any?>>
                    documents.map { it as Map<String, Any?> }
                }
    }

    private fun convertInput(data: Any): List<Map<String, Any?>> {
        return when (data) {
            is Map<*, *> -> convertInputFromMap(data as Map<String, Any>)
            is Collection<*> -> convertInputFromCollection(data)
            is String -> convertInputFromMap(mapOf("id" to 1, "text" to data))
            else -> throw java.lang.RuntimeException("Class ${data::class.java.name} not supported")
        }
    }

    private fun convertInputFromCollection(data: Collection<*>): List<Map<String, Any?>> {
        if (data.isEmpty()) {
            return emptyList()
        }
        return data.filterNotNull().mapIndexed { index, element ->
            when (element) {
                is Map<*, *> -> element as Map<String, Any>
                is String -> mapOf("id" to index.toString(), "text" to element)
                else -> throw java.lang.RuntimeException("Class ${element::class.java.name} not supported")
            }
        }
    }

    private fun convertInputFromMap(data: Map<String, Any>): List<Map<String, Any?>> {
        if (data.isEmpty()) {
            return emptyList()
        }
        return listOf(data)
    }

    override fun entities(data: Any, config: Map<String, Any?>): List<Map<String, Any?>> = postData(AzureEndpoint.ENTITIES.method, key, data)

    override fun sentiment(data: Any, config: Map<String, Any?>): List<MapResult> = postData(AzureEndpoint.SENTIMENT.method, key, data).map { item -> MapResult(item)}

    override fun keyPhrases(data: Any, config: Map<String, Any?>): List<MapResult> = postData(AzureEndpoint.KEY_PHRASES.method, key, data).map { item -> MapResult(item)}

    override fun vision(data: Any, config: Map<String, Any?>): List<MapResult> = postData(AzureEndpoint.VISION.method, key, data, config).map { item -> MapResult(item)}
}