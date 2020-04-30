package apoc.nlp

import apoc.graph.document.builder.DocumentToGraph
import apoc.graph.util.GraphsConfig
import apoc.result.VirtualGraph
import apoc.result.VirtualNode
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.Relationship
import org.neo4j.graphdb.RelationshipType
import org.neo4j.graphdb.Transaction

abstract class AWSVirtualGraph(private val sourceNodes: List<Node>, val relationshipType: RelationshipType, val mapping: Map<String, String>) {
    fun createAndStore(transaction: Transaction?): VirtualGraph {
        return createVirtualGraph(transaction)
    }

    fun create(): VirtualGraph {
        return createVirtualGraph(null)
    }

    abstract fun extractDocument(index: Int, sourceNode: Node) : Any?


    open fun createVirtualGraph(transaction: Transaction?): VirtualGraph {
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
            val document = extractDocument(index, sourceNode)

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
}