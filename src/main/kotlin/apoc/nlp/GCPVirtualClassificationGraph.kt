package apoc.nlp

import apoc.result.NodeValueErrorMapResult
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.RelationshipType

data class GCPVirtualClassificationGraph(private val results: List<NodeValueErrorMapResult>, private val sourceNodes: List<Node>, val relType: RelationshipType): NLPVirtualGraph(sourceNodes, relType, ENTITY_MAPPING) {
    override fun extractDocument(index: Int, sourceNode: Node) : Any? = results.get(index).value["categories"]

    companion object {
        val ENTITY_MAPPING = mapOf("$" to "Category{!name,type,@metadata}")
    }
}