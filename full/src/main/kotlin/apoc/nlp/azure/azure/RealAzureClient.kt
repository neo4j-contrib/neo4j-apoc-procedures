package apoc.nlp.azure

import apoc.result.NodeWithMapResult
import apoc.util.JsonUtil
import org.neo4j.graphdb.Node
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

class RealAzureClient(private val baseUrl: String, private val key: String, private val log: Log, val config: Map<String, Any>) : AzureClient {

    private val nodeProperty = config.getOrDefault("nodeProperty", "text").toString()

    companion object {
        fun responseToNodeWithMapResult(resp: Map<String, Any>, source: List<Node>): NodeWithMapResult {
            val nodeId = resp.getValue("id").toString().toLong()
            val node = source.find { it.id == nodeId }
            return NodeWithMapResult(node, resp, emptyMap())
        }
    }

    private fun postData(method: String, subscriptionKeyValue: String, data: List<Map<String, Any>>, config: Map<String, Any> = emptyMap()): List<Map<String, Any>> {
        val fullUrl = baseUrl + method + config.map { "${it.key}=${it.value}" }
                .joinToString("&")
                .also { if (it.isNullOrBlank()) it else "?$it" }
        val url = URL(fullUrl)
        return postData(url, subscriptionKeyValue, data)
    }

    private fun postData(url: URL, subscriptionKeyValue: String, data: List<Map<String, Any>>): List<Map<String, Any>> {
        val connection = url.openConnection() as HttpsURLConnection
        connection.requestMethod = "POST"
        connection.setRequestProperty("Ocp-Apim-Subscription-Key", subscriptionKeyValue)
        connection.doOutput = true

        connection.setRequestProperty("Content-Type", "application/json")
        DataOutputStream(connection.outputStream).use { it.write(JsonUtil.writeValueAsBytes(mapOf("documents" to convertInput(data)))) }

        return connection.inputStream
                .use { JsonUtil.OBJECT_MAPPER.readValue(it, Any::class.java) }
                .let { result ->
                    val documents = (result as Map<String, Any?>)["documents"] as List<Map<String, Any?>>
                    documents.map { it as Map<String, Any> }
                }
    }

    private fun convertInput(data: Any): List<Map<String, Any>> {
        return when (data) {
            is Map<*, *> -> listOf(data as Map<String, Any>)
            is Collection<*> -> data.filterNotNull().map { convertInput(it) }.flatten()
            is String -> convertInput(mapOf("id" to 1, "text" to data))
            else -> throw java.lang.RuntimeException("Class ${data::class.java.name} not supported")
        }
    }

    override fun entities(nodes: List<Node>, batchId: Int): List<Map<String, Any>> {
        val data = nodes.map { node -> mapOf("id" to node.id, "text" to node.getProperty(nodeProperty)) }
        return postData(AzureEndpoint.ENTITIES.method, key, data)
    }

    override fun sentiment(nodes: List<Node>, batchId: Int): List<Map<String, Any>>  {
        val data = nodes.map { node -> mapOf("id" to node.id, "text" to node.getProperty(nodeProperty)) }
        val result = postData(AzureEndpoint.SENTIMENT.method, key, data)
        return result
    }

    override fun keyPhrases(nodes: List<Node>, batchId: Int): List<Map<String, Any>> {
        val data = nodes.map { node -> mapOf("id" to node.id, "text" to node.getProperty(nodeProperty)) }
        val result = postData(AzureEndpoint.KEY_PHRASES.method, key, data)
        return result
    }

}