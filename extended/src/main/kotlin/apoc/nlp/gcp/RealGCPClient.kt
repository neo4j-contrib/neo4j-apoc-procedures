package apoc.nlp.gcp

import apoc.result.NodeValueErrorMapResult
import apoc.util.JsonUtil
import org.neo4j.graphdb.Node
import org.neo4j.logging.Log
import java.io.DataOutputStream
import java.net.URL
import javax.net.ssl.HttpsURLConnection


enum class GCPEndpoint(val method: String) {
    CLASSIFY("/documents:classifyText"),
    SENTIMENT("/documents:analyzeSentiment"),
    ENTITIES("/documents:analyzeEntities")
}

class RealGCPClient(config: Map<String, Any>, private val log: Log) : GCPClient {
    private val baseUrl = "https://language.googleapis.com/v1"
    private val apiKey = config["key"].toString()
    private val nodeProperty = config.getOrDefault("nodeProperty", "text").toString()

    companion object {
        @JvmStatic val MAPPER = JsonUtil.OBJECT_MAPPER!!
    }

    private fun postData(method: String, data: String): Map<String, Any?> {
        val fullUrl = "$baseUrl$method?key=$apiKey"
        val url = URL(fullUrl)
        return postData(url, data)
    }

    private fun postData(url: URL, data: String): Map<String, Any?> {
        val connection = url.openConnection() as HttpsURLConnection
        connection.requestMethod = "POST"
        connection.doOutput = true

        connection.setRequestProperty("Content-Type", "application/json")
        DataOutputStream(connection.outputStream).use { it.write(MAPPER.writeValueAsBytes(mapOf("document" to convertInput(data)))) }

        return connection.inputStream
                .use { MAPPER.readValue(it, Any::class.java) }
                .let {result ->
                    (result as Map<String, Any?>)
                }
    }

    private fun convertInput(data: String): Map<String, Any?> {
        return mapOf("type" to "PLAIN_TEXT", "content" to data)
    }

    override fun entities(nodes: List<Node>, batchId: Int): List<NodeValueErrorMapResult> {
        return nodes.map { node -> NodeValueErrorMapResult.withResult(node, postData(GCPEndpoint.ENTITIES.method, node.getProperty(nodeProperty).toString())) }
    }

    override fun classify(nodes: List<Node>, batchId: Int): List<NodeValueErrorMapResult> {
        return nodes.map {node -> NodeValueErrorMapResult.withResult(node, postData(GCPEndpoint.CLASSIFY.method, node.getProperty(nodeProperty).toString())) }
    }
}

interface GCPClient {
    fun entities(nodes: List<Node>, batchId: Int): List<NodeValueErrorMapResult>
    fun classify(nodes: List<Node>, batchId: Int): List<NodeValueErrorMapResult>
}