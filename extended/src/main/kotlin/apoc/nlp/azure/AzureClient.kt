package apoc.nlp.azure

import org.neo4j.graphdb.Node

interface AzureClient {
    fun entities(nodes: List<Node>, batchId: Int): List<Map<String, Any>>

    fun sentiment(nodes: List<Node>, batchId: Int): List<Map<String, Any>>


    fun keyPhrases(nodes: List<Node>, batchId: Int): List<Map<String, Any>>
}
