package apoc.nlp

import apoc.graph.document.builder.DocumentToGraph
import apoc.graph.util.GraphsConfig
import apoc.nlp.aws.AWSProcedures
import apoc.result.VirtualGraph
import apoc.result.VirtualNode
import com.amazonaws.services.comprehend.model.BatchDetectEntitiesResult
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.Relationship
import org.neo4j.graphdb.RelationshipType
import org.neo4j.graphdb.Transaction

class VirtualNLPGraph(private val detectEntitiesResult: BatchDetectEntitiesResult, private val sourceNodes: List<Node>, val relationshipType: RelationshipType, val mapping: Map<String, String>) {
        fun createAndStore(transaction: Transaction?): VirtualGraph {
           return createVirtualGraph(true, transaction)
        }

        fun create(): VirtualGraph {
            return createVirtualGraph(false, null)
        }

    private fun createVirtualGraph(storeGraph: Boolean, transaction:Transaction?) : VirtualGraph
    {
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
            val document = AWSProcedures.transformResults(index, sourceNode, detectEntitiesResult).value["entities"]
            val graph = documentToGraph.create(document)
            val mutableGraph = graph.graph.toMutableMap()

            val nodes = (mutableGraph["nodes"] as Set<Node>).toMutableSet()
            nonSourceNodes.addAll(nodes)

            val relationships = (mutableGraph["relationships"] as Set<Relationship>).toMutableSet()
            val node = if (storeGraph) {
                NLPHelperFunctions.mergeRelationships(transaction!!, sourceNode, nodes, relationshipType).forEach { rel -> relationships.add(rel) }
                sourceNode
            } else {
                val virtualNode = VirtualNode(sourceNode, sourceNode.propertyKeys.toList())
                NLPHelperFunctions.createRelationships(virtualNode, nodes, relationshipType).forEach { rel -> relationships.add(rel) }
                virtualNode
            }
            nodes.add(node)

            allNodes.addAll(nodes)
            allRelationships.addAll(relationships)
        }

        return VirtualGraph("Graph", allNodes, allRelationships, emptyMap())
    }

        private fun createGraphConfig(mappings: Map<String, String>, write: Boolean): Map<String, Any> {
            return mapOf(
                    "skipValidation" to true,
                    "mappings" to mappings,
                    "write" to write
            )
        }
}