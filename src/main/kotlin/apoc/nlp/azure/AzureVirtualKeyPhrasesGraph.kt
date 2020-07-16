package apoc.nlp.azure

import apoc.graph.document.builder.DocumentToGraph
import apoc.nlp.NLPHelperFunctions
import apoc.nlp.NLPVirtualGraph
import apoc.result.NodeValueErrorMapResult
import apoc.result.VirtualGraph
import apoc.result.VirtualNode
import org.apache.commons.text.WordUtils
import org.neo4j.graphdb.*
import java.util.LinkedHashMap

data class AzureVirtualKeyPhrasesGraph(private val results: List<Map<String, Any>>, private val sourceNodes: List<Node>, val relType: RelationshipType): NLPVirtualGraph() {
    override fun extractDocument(index: Int, sourceNode: Node) : Any? = extractDocument(-1, sourceNode)
    private fun extractDocument(sourceNode: Node) : Any? = results.find { result -> result["id"] == sourceNode.id.toString()  }!!["keyPhrases"]

    companion object {
        val LABEL = Label { "KeyPhrase" }
    }

    override fun createVirtualGraph(transaction: Transaction?): VirtualGraph {
        val storeGraph = transaction != null

        val allNodes: MutableSet<Node> = mutableSetOf()
        val nonSourceNodes: MutableSet<Node> = mutableSetOf()
        val allRelationships: MutableSet<Relationship> = mutableSetOf()

        sourceNodes.forEach { sourceNode ->
            val document = extractDocument(sourceNode) as List<String>
            val virtualNodes = LinkedHashMap<MutableSet<String>, MutableSet<Node>>()
            val virtualNode = VirtualNode(sourceNode, sourceNode.propertyKeys.toList())

            val documentToNodes = DocumentToGraph.DocumentToNodes(nonSourceNodes, transaction)
            val entityNodes = mutableSetOf<Node>()
            val relationships = mutableSetOf<Relationship>()
            for (item in document) {
                val labels: Array<Label> = labels()
                val idValues: Map<String, Any> = mapOf("text" to item)

                val node = if (storeGraph) {
                    val entityNode = documentToNodes.getOrCreateRealNode(labels, idValues)
                    entityNode.setProperty("text", item)

                    entityNodes.add(entityNode)
                    relationships.add(NLPHelperFunctions.mergeRelationship(sourceNode, entityNode, relType))

                    sourceNode
                } else {
                    val entityNode = documentToNodes.getOrCreateVirtualNode(virtualNodes, labels, idValues)
                    entityNode.setProperty("text", item)
                    entityNodes.add(entityNode)

                    DocumentToGraph.getNodesWithSameLabels(virtualNodes, labels).add(entityNode)

                    relationships.add(NLPHelperFunctions.mergeRelationship(virtualNode, entityNode, relType))

                    virtualNode
                }
                allNodes.add(node)
            }

            nonSourceNodes.addAll(entityNodes)
            allNodes.addAll(entityNodes)
            allRelationships.addAll(relationships)
        }

        return VirtualGraph("Graph", allNodes, allRelationships, emptyMap())
    }

    private fun labels() = arrayOf(LABEL)

}