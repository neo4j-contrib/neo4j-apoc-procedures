package apoc.nlp

import apoc.graph.document.builder.DocumentToGraph
import apoc.graph.util.GraphsConfig
import apoc.nlp.aws.AWSProcedures
import apoc.result.VirtualGraph
import apoc.result.VirtualNode
import com.amazonaws.services.comprehend.model.BatchDetectEntitiesResult
import com.amazonaws.services.comprehend.model.BatchDetectKeyPhrasesResult
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.Relationship
import org.neo4j.graphdb.RelationshipType
import org.neo4j.graphdb.Transaction

data class AWSVirtualKeyPhrasesGraph(private val detectEntitiesResult: BatchDetectKeyPhrasesResult, private val sourceNodes: List<Node>, val relationshipType: RelationshipType, val mapping: Map<String, String>) {
    fun createAndStore(transaction: Transaction?): VirtualGraph {
        return createVirtualGraph(transaction)
    }

    fun create(): VirtualGraph {
        return createVirtualGraph(null)
    }

    private fun createVirtualGraph(transaction: Transaction?): VirtualGraph {
        val storeGraph = transaction != null
        val graphConfig = mapOf(
                "skipValidation" to true,
                "mappings" to mapping,
                "write" to storeGraph
        )

        val allNodes: MutableSet<Node> = mutableSetOf()
        val nonSourceNodes: MutableSet<Node> = mutableSetOf()
        val allRelationships: MutableSet<Relationship> = mutableSetOf()

        sourceNodes.forEachIndexed { index, sourceNode ->
            val documentToGraph = DocumentToGraph(transaction, GraphsConfig(graphConfig), nonSourceNodes)
            val document = AWSProcedures.transformResults(index, sourceNode, detectEntitiesResult).value["keyPhrases"]

            val graph = documentToGraph.create(document).graph

            val nodes = (graph["nodes"] as Set<Node>)
            nonSourceNodes.addAll(nodes)

            val relationships = (graph["relationships"] as Set<Relationship>).toMutableSet()
            val node = if (storeGraph) {
                NLPHelperFunctions.mergeRelationships(transaction!!, sourceNode, nodes, relationshipType).forEach { rel -> relationships.add(rel) }
                sourceNode
            } else {
                val virtualNode = VirtualNode(sourceNode, sourceNode.propertyKeys.toList())
                NLPHelperFunctions.createRelationships(virtualNode, nodes, relationshipType).forEach { rel -> relationships.add(rel) }
                virtualNode
            }
            allNodes.add(node)
            allNodes.addAll(nodes)
            allRelationships.addAll(relationships)
        }

        return VirtualGraph("Graph", allNodes, allRelationships, emptyMap())
    }

    companion object {
        val KEY_PHRASE_MAPPING = mapOf("$" to "KeyPhrase{!text,@metadata}")
    }
}