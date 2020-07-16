package apoc.nlp.azure

import org.neo4j.graphdb.Node
import org.neo4j.logging.Log

class DummyAzureClient(config: Map<String, Any>, private val log: Log) : AzureClient {
    private val nodeProperty = config.getOrDefault("nodeProperty", "text").toString()

    private fun convertInput(data: List<Node>): List<String> {
        return data.map { node -> node.getProperty(nodeProperty).toString() }
    }

    override fun entities(nodes: List<Node>, batchId: Int): List<Map<String, Any>> {
        val batchResults: MutableList<Map<String, Any>> = mutableListOf()

        nodes.map { node ->
            val nodeId = node.id
            val value = mapOf("id" to nodeId, "entities" to listOf(
                    mapOf("name" to  "token-1-node-${nodeId}-batch-${batchId}", "type" to "Location"),
                    mapOf("name" to  "token-2-node-${nodeId}-batch-${batchId}", "type" to "DateTime")
            ))
            batchResults += value
        }
        return batchResults
    }

    override fun sentiment(nodes: List<Node>, batchId: Int): List<Map<String, Any>> {
        TODO("Not yet implemented")
    }

    override fun keyPhrases(nodes: List<Node>, batchId: Int): List<Map<String, Any>> {
        val batchResults: MutableList<Map<String, Any>> = mutableListOf()

        nodes.map { node ->
            val nodeId = node.id
            val value = mapOf("id" to nodeId, "keyPhrases" to listOf(
                    "keyPhrase-1-node-${nodeId}-batch-${batchId}",
                    "keyPhrase-2-node-${nodeId}-batch-${batchId}"
            ))
            batchResults += value
        }
        return batchResults

    }
}
