package apoc.nlp

import apoc.result.VirtualNode
import org.neo4j.graphdb.Node
import org.neo4j.test.rule.DbmsRule

object NplUtils {

    fun commonNlpInit(neo4j: DbmsRule, query: String): Triple<Node, Node, NodeMatcher> =
        neo4j.executeTransactionally(query, emptyMap()) {
            val sourceNode = it.next()["a"] as Node
            val nodeMatcher = NodeMatcher(sourceNode)
            val virtualSourceNode = VirtualNode(sourceNode, sourceNode.propertyKeys.toList())
            return@executeTransactionally Triple(sourceNode, virtualSourceNode, nodeMatcher)
        }
}