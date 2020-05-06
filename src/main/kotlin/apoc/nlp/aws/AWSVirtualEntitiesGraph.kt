package apoc.nlp.aws

import apoc.nlp.NLPVirtualGraph
import com.amazonaws.services.comprehend.model.BatchDetectEntitiesResult
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.RelationshipType

data class AWSVirtualEntitiesGraph(private val detectEntitiesResult: BatchDetectEntitiesResult, private val sourceNodes: List<Node>, val relType: RelationshipType): NLPVirtualGraph(sourceNodes, relType, ENTITY_MAPPING) {
    override fun extractDocument(index: Int, sourceNode: Node) : Any? = AWSProcedures.transformResults(index, sourceNode, detectEntitiesResult).value["entities"]

    companion object {
        val ENTITY_MAPPING = mapOf("$" to "Entity{!text,type,@metadata}")
    }
}