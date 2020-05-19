package apoc.nlp.aws

import apoc.graph.document.builder.DocumentToGraph
import apoc.nlp.NLPHelperFunctions.Companion.createRelationship
import apoc.nlp.NLPHelperFunctions.Companion.mergeRelationship
import apoc.nlp.NLPVirtualGraph
import apoc.result.VirtualGraph
import apoc.result.VirtualNode
import com.amazonaws.services.comprehend.model.BatchDetectKeyPhrasesResult
import org.neo4j.graphdb.*

data class AWSVirtualKeyPhrasesGraph(private val detectEntitiesResult: BatchDetectKeyPhrasesResult, private val sourceNodes: List<Node>, val relType: RelationshipType): NLPVirtualGraph() {
    override fun extractDocument(index: Int, sourceNode: Node) : ArrayList<Map<String, Any>> = AWSProcedures.transformResults(index, sourceNode, detectEntitiesResult).value["keyPhrases"] as ArrayList<Map<String, Any>>

    companion object {
        val KEYS = listOf("text")
        val LABEL = Label { "KeyPhrase" }
        val ID_KEYS = listOf("text")
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
                val labels: Array<Label> = arrayOf(LABEL)
                val idValues: Map<String, Any> = ID_KEYS.map { it to item[it].toString() }.toMap()

                val node = if (storeGraph) {
                    val entityNode = documentToNodes.getOrCreateRealNode(labels, idValues)
                    setProperties(entityNode, item)
                    entityNodes.add(entityNode)

                    val nodeAndScore = Pair(entityNode, item["score"] as Float)
                    mergeRelationship(transaction!!, sourceNode, nodeAndScore, relType).forEach { rel -> relationships.add(rel) }

                    sourceNode
                } else {
                    val entityNode = documentToNodes.getOrCreateVirtualNode(LinkedHashMap(), labels, idValues)
                    setProperties(entityNode, item)
                    entityNodes.add(entityNode)

                    val virtualNode = VirtualNode(sourceNode, sourceNode.propertyKeys.toList())
                    val nodeAndScore = Pair(entityNode, item["score"] as Float)
                    relationships.add(createRelationship(virtualNode, nodeAndScore, relType))

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

    private fun setProperties(entityNode: Node, item: Map<String, Any>) {
        KEYS.forEach { key -> entityNode.setProperty(key, item[key].toString()) }
    }
}

