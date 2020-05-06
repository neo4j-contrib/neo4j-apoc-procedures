package apoc.nlp.aws

import apoc.nlp.NLPVirtualGraph
import com.amazonaws.services.comprehend.model.BatchDetectKeyPhrasesResult
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.RelationshipType

data class AWSVirtualKeyPhrasesGraph(private val detectEntitiesResult: BatchDetectKeyPhrasesResult, private val sourceNodes: List<Node>, val relType: RelationshipType): NLPVirtualGraph(sourceNodes, relType, KEY_PHRASE_MAPPING) {
    override fun extractDocument(index: Int, sourceNode: Node) : Any? = AWSProcedures.transformResults(index, sourceNode, detectEntitiesResult).value["keyPhrases"]

    companion object {
        val KEY_PHRASE_MAPPING = mapOf("$" to "KeyPhrase{!text,@metadata}")
    }
}

