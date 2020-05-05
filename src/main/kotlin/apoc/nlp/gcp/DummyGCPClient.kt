package apoc.nlp.gcp

import apoc.result.NodeValueErrorMapResult
import org.neo4j.graphdb.Node
import org.neo4j.logging.Log

class DummyGCPClient(config: Map<String, Any>, private val log: Log) : GCPClient {
    private val nodeProperty = config.getOrDefault("nodeProperty", "text").toString()

    override fun entities(nodes: List<Node>, batchId: Int): List<NodeValueErrorMapResult> {
        val batchResults: MutableList<NodeValueErrorMapResult> = mutableListOf()

        nodes.mapIndexed { index, node ->
            val value = mapOf("entities" to listOf(
                    mapOf("name" to  "token-1-index-${index}-batch-${batchId}", "type" to "CONSUMER_GOOD", "salience" to 0.1),
                    mapOf("name" to  "token-2-index-${index}-batch-${batchId}", "type" to "LOCATION", "salience" to 0.2)
            ))
            batchResults += NodeValueErrorMapResult.withResult(node, value)
        }

        return batchResults
    }

    override fun classify(nodes: List<Node>, batchId: Int): List<NodeValueErrorMapResult> {
        val batchResults: MutableList<NodeValueErrorMapResult> = mutableListOf()

        nodes.mapIndexed { index, node ->
            val value = mapOf("categories" to listOf(
                    mapOf("name" to  "category-1-index-${index}-batch-${batchId}", "confidence" to 0.1),
                    mapOf("name" to  "category-2-index-${index}-batch-${batchId}", "confidence" to 0.2)
            ))
            batchResults += NodeValueErrorMapResult.withResult(node, value)
        }

        return batchResults
    }


    private fun convertInput(data: List<Node>): List<String> {
        return data.map { node -> node.getProperty(nodeProperty).toString() }
    }

}
