package apoc.nlp.gcp

import apoc.graph.document.builder.DocumentToGraph
import apoc.nlp.NLPHelperFunctions
import apoc.nlp.NLPVirtualGraph
import apoc.result.NodeValueErrorMapResult
import apoc.result.VirtualGraph
import apoc.result.VirtualNode
import org.apache.commons.text.WordUtils
import org.neo4j.graphdb.*
import java.util.LinkedHashMap

data class GCPVirtualEntitiesGraph(private val results: List<NodeValueErrorMapResult>, private val sourceNodes: List<Node>, val relType: RelationshipType): NLPVirtualGraph() {
    override fun extractDocument(index: Int, sourceNode: Node) : Any? = results[index].value["entities"]

    companion object {
        const val LABEL_KEY = "type"
        val LABEL = Label { "Entity" }
        val KEY_MAPPINGS = mapOf("name" to "text", "type" to "type")
        val ID_MAPPINGS = mapOf("name" to "text")
    }

    override fun createVirtualGraph(transaction: Transaction?): VirtualGraph {
        val storeGraph = transaction != null

        val allNodes: MutableSet<Node> = mutableSetOf()
        val nonSourceNodes: MutableSet<Node> = mutableSetOf()
        val allRelationships: MutableSet<Relationship> = mutableSetOf()

        sourceNodes.forEachIndexed { index, sourceNode ->
            val document = extractDocument(index, sourceNode) as List<Map<String, Any>>

            val documentToNodes = DocumentToGraph.DocumentToNodes(nonSourceNodes, transaction)
            val entityNodes = mutableSetOf<Node>()
            val relationships = mutableSetOf<Relationship>()
            for (item in document) {
                val labels: Array<Label> = arrayOf(LABEL, Label { asLabel(item[LABEL_KEY].toString()) })
                val idValues: Map<String, Any> = ID_MAPPINGS.map { (key, value) -> value to item[key].toString() }.toMap()

                val node = if (storeGraph) {
                    val entityNode = documentToNodes.getOrCreateRealNode(labels, idValues)
                    setProperties(entityNode, item)
                    entityNodes.add(entityNode)

                    val nodeAndScore = Pair(entityNode, item["salience"] as Float)
                    NLPHelperFunctions.mergeRelationship(transaction!!, sourceNode, nodeAndScore, relType).forEach { rel -> relationships.add(rel) }

                    sourceNode
                } else {
                    val entityNode = documentToNodes.getOrCreateVirtualNode(LinkedHashMap(), labels, idValues)
                    setProperties(entityNode, item)
                    entityNodes.add(entityNode)

                    val virtualNode = VirtualNode(sourceNode, sourceNode.propertyKeys.toList())
                    val nodeAndScore = Pair(entityNode, item["salience"] as Number)
                    relationships.add(NLPHelperFunctions.createRelationship(virtualNode, nodeAndScore, relType))

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

    private fun asLabel(value: String) : String {
        return WordUtils.capitalizeFully(value, '_', ' ')
                .replace("_".toRegex(), "")
                .replace(" ".toRegex(), "")
    }

    private fun setProperties(entityNode: Node, item: Map<String, Any>) {
        KEY_MAPPINGS.forEach { (key, value) ->
            entityNode.setProperty(value, item[key].toString())
        }
    }
}