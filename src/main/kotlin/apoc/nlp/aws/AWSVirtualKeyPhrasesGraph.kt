package apoc.nlp.aws

import apoc.graph.document.builder.DocumentToGraph
import apoc.nlp.NLPHelperFunctions
import apoc.nlp.NLPVirtualGraph
import apoc.result.VirtualGraph
import apoc.result.VirtualNode
import com.amazonaws.services.comprehend.model.BatchDetectKeyPhrasesResult
import org.neo4j.graphdb.Label
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.Relationship
import org.neo4j.graphdb.RelationshipType
import org.neo4j.graphdb.Transaction

data class AWSVirtualKeyPhrasesGraph(private val detectEntitiesResult: BatchDetectKeyPhrasesResult, private val sourceNodes: List<Node>, val relType: RelationshipType, val relProperty: String, val cutoff: Number): NLPVirtualGraph() {
    override fun extractDocument(index: Int, sourceNode: Node) : ArrayList<Map<String, Any>> = AWSProcedures.transformResults(index, sourceNode, detectEntitiesResult).value["keyPhrases"] as ArrayList<Map<String, Any>>

    companion object {
        const val SCORE_PROPERTY = "score"
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
            val virtualNodes = LinkedHashMap<MutableSet<String>, MutableSet<Node>>()
            val virtualNode = VirtualNode(sourceNode, sourceNode.propertyKeys.toList())

            val documentToNodes = DocumentToGraph.DocumentToNodes(nonSourceNodes, transaction)
            val entityNodes = mutableSetOf<Node>()
            val relationships = mutableSetOf<Relationship>()
            for (item in document) {
                val labels: Array<Label> = labels(item)
                val idValues: Map<String, Any> = idValues(item)

                val score = item[SCORE_PROPERTY] as Number
                if(score.toDouble() >= cutoff.toDouble()) {
                    val node = if (storeGraph) {
                        val keyPhraseNode = documentToNodes.getOrCreateRealNode(labels, idValues)
                        setProperties(keyPhraseNode, item)
                        entityNodes.add(keyPhraseNode)

                        val nodeAndScore = Pair(keyPhraseNode, score)
                        relationships.add(NLPHelperFunctions.mergeRelationship(sourceNode, nodeAndScore, relType, relProperty))

                        sourceNode
                    } else {
                        val keyPhraseNode = documentToNodes.getOrCreateVirtualNode(virtualNodes, labels, idValues)
                        setProperties(keyPhraseNode, item)
                        entityNodes.add(keyPhraseNode)

                        DocumentToGraph.getNodesWithSameLabels(virtualNodes, labels).add(keyPhraseNode)

                        val nodeAndScore = Pair(keyPhraseNode, score)
                        relationships.add(NLPHelperFunctions.mergeRelationship(virtualNode, nodeAndScore, relType, relProperty))

                        virtualNode
                    }
                    allNodes.add(node)
                }
            }

            nonSourceNodes.addAll(entityNodes)
            allNodes.addAll(entityNodes)
            allRelationships.addAll(relationships)
        }

        return VirtualGraph("Graph", allNodes, allRelationships, emptyMap())
    }

    private fun idValues(item: Map<String, Any>) = ID_KEYS.map { it to item[it].toString() }.toMap()

    private fun labels(item: Map<String, Any>) = arrayOf(LABEL)

    private fun setProperties(entityNode: Node, item: Map<String, Any>) {
        KEYS.forEach { key -> entityNode.setProperty(key, item[key].toString()) }
    }
}

