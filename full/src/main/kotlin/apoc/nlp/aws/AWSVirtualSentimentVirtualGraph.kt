package apoc.nlp.aws

import apoc.nlp.NLPVirtualGraph
import apoc.result.VirtualGraph
import apoc.result.VirtualNode
import com.amazonaws.services.comprehend.model.BatchDetectSentimentResult
import org.apache.commons.lang.WordUtils
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.Relationship
import org.neo4j.graphdb.RelationshipType
import org.neo4j.graphdb.Transaction

data class AWSVirtualSentimentVirtualGraph(private val detectEntitiesResult: BatchDetectSentimentResult, private val sourceNodes: List<Node>): NLPVirtualGraph() {
    override fun extractDocument(index: Int, sourceNode: Node) : Any? = AWSProcedures.transformResults(index, sourceNode, detectEntitiesResult).value

    override fun createVirtualGraph(transaction: Transaction?): VirtualGraph {
        val storeGraph = transaction != null

        val allNodes: MutableSet<Node> = mutableSetOf()
        val allRelationships: MutableSet<Relationship> = mutableSetOf()

        sourceNodes.forEachIndexed { index, sourceNode ->
            val document = extractDocument(index, sourceNode) as Map<String, Any>
            val (sentiment, score ) = extractSentiment(document)

            val node = if (storeGraph) {
                sourceNode.setProperty("sentiment", sentiment)
                sourceNode.setProperty("sentimentScore", score)
                sourceNode
            } else {
                val virtualNode = VirtualNode(sourceNode, sourceNode.propertyKeys.toList())
                virtualNode.setProperty("sentiment", sentiment)
                virtualNode.setProperty("sentimentScore", score)
                virtualNode
            }

            allNodes.add(node)
        }

        return VirtualGraph("Graph", allNodes, allRelationships, emptyMap())
    }

    companion object {
        fun extractSentiment(value: Map<String, Any>) : Pair<String, Float> {
            val sentiment = WordUtils.capitalizeFully(value["sentiment"] as String)
            val sentimentScore = value["sentimentScore"] as Map<String, Any>

            return Pair(sentiment, sentimentScore[sentiment.toLowerCase()] as Float)
        }
    }
}