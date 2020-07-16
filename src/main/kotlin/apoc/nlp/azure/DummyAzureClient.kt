package apoc.nlp.azure

import org.neo4j.graphdb.Node
import org.neo4j.logging.Log

class DummyAzureClient(config: Map<String, Any>, private val log: Log) : AzureClient {
    private val nodeProperty = config.getOrDefault("nodeProperty", "text").toString()

    private fun convertInput(data: List<Node>): List<String> {
        return data.map { node -> node.getProperty(nodeProperty).toString() }
    }

    override fun entities(nodes: List<Node>): List<Map<String, Any>> {
        TODO("Not yet implemented")
    }

    override fun sentiment(nodes: List<Node>): List<Map<String, Any>> {
        TODO("Not yet implemented")
    }

    override fun keyPhrases(nodes: List<Node>): List<Map<String, Any>> {
        TODO("Not yet implemented")
    }
}
