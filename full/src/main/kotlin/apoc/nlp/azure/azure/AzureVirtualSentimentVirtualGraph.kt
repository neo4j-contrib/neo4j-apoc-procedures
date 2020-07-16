package apoc.nlp.azure

import apoc.nlp.NLPVirtualGraph
import apoc.result.VirtualGraph
import apoc.result.VirtualNode
import com.amazonaws.services.comprehend.model.BatchDetectSentimentResult
import org.apache.commons.lang.WordUtils
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.Relationship
import org.neo4j.graphdb.RelationshipType
import org.neo4j.graphdb.Transaction

data class AzureVirtualSentimentVirtualGraph(private val results: List<Map<String, Any>>, private val sourceNodes: List<Node>): NLPVirtualGraph() {
    override fun extractDocument(index: Int, sourceNode: Node) : Any? = extractDocument(-1, sourceNode)
    private fun extractDocument(sourceNode: Node) : Any? = results.find { result -> result["id"] == sourceNode.id.toString()  }

    override fun createVirtualGraph(transaction: Transaction?): VirtualGraph {
        val storeGraph = transaction != null

        val allNodes: MutableSet<Node> = mutableSetOf()
        val allRelationships: MutableSet<Relationship> = mutableSetOf()

        sourceNodes.forEach { sourceNode ->
            val document = extractDocument(sourceNode) as Map<String, Any>
            val score = document["score"] as Number

            val node = if (storeGraph) {
                sourceNode.setProperty("sentimentScore", score)
                sourceNode
            } else {
                val virtualNode = VirtualNode(sourceNode, sourceNode.propertyKeys.toList())
                virtualNode.setProperty("sentimentScore", score)
                virtualNode
            }

            allNodes.add(node)
        }

        return VirtualGraph("Graph", allNodes, allRelationships, emptyMap())
    }
}