package apoc.nlp.azure

import org.neo4j.graphdb.Node

interface AzureClient {
    fun entities(nodes: List<Node>): List<Map<String, Any>>

    fun sentiment(nodes: List<Node>): List<Map<String, Any>>

    fun keyPhrases(nodes: List<Node>): List<Map<String, Any>>
}
