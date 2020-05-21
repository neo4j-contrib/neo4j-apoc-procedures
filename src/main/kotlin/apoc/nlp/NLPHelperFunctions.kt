package apoc.nlp

import apoc.util.Util
import org.neo4j.graphdb.*
import java.util.stream.Stream
import kotlin.streams.asStream

class NLPHelperFunctions {
    companion object {
        fun createRelationship(node: Node, nodesAndScore: Pair<Node, Number>, relationshipType: RelationshipType, relProperty: String): Relationship {
            val existingRelationships = node.getRelationships(Direction.OUTGOING, relationshipType).asSequence().asStream()
            val potentialRelationship = existingRelationships.filter { r -> r.endNode == nodesAndScore.first }.findFirst()

            return if(potentialRelationship.isPresent) {
                val relationship = potentialRelationship.get()
                if(nodesAndScore.second.toDouble() > (relationship.getProperty(relProperty) as Number).toDouble()) {
                    relationship.setProperty(relProperty, nodesAndScore.second)
                }
                relationship
            } else {
                val relationship = node.createRelationshipTo(nodesAndScore.first, relationshipType)
                relationship.setProperty(relProperty, nodesAndScore.second)
                relationship
            }
        }

        fun mergeRelationship(transaction: Transaction, node: Node, nodeAndScores: Pair<Node, Number>, relType: RelationshipType, relProperty: String): Stream<Relationship> {
            val cypher = """WITH ${'$'}startNode as startNode, ${'$'}endNode as endNode, ${'$'}score as score
            MERGE (startNode)-[r:${Util.quote(relType.name())}]->(endNode)
            
            FOREACH(ignoreMe In CASE WHEN score > coalesce(r.${relProperty}, 0.0) THEN [1] ELSE [] END | 
                SET r.${relProperty} = score
            )
   
            RETURN r"""

            val params = mapOf("startNode" to node, "endNode" to nodeAndScores.first, "score" to nodeAndScores.second)
            return transaction.execute(cypher, params).columnAs<Relationship>("r").stream()
        }

        fun entityRelationshipType(config: Map<String, Any>): RelationshipType  {
            val selectedType = getSelectedType(config, listOf("writeRelationshipType", "relationshipType"), "ENTITY")
            return RelationshipType.withName(selectedType.toString())
        }

        fun categoryRelationshipType(config: Map<String, Any>): RelationshipType {
            val selectedType = getSelectedType(config, listOf("writeRelationshipType", "relationshipType"), "CATEGORY")
            return RelationshipType.withName(selectedType.toString())
        }

        fun keyPhraseRelationshipType(config: Map<String, Any>): RelationshipType {
            val selectedType = getSelectedType(config, listOf("writeRelationshipType", "relationshipType"), "KEY_PHRASE")
            return RelationshipType.withName(selectedType.toString())
        }
        fun getNodeProperty(config: Map<String, Any>): String {
            return config.getOrDefault("nodeProperty", "text").toString()
        }

        private fun getSelectedType(config: Map<String, Any>, keys: List<String>, default: String) =
                (keys.map { key -> config[key] } + default).filterNotNull().first()

        fun verifySource(source: Any) {
            when (source) {
                is Node -> return
                is List<*> -> source.forEach { item ->
                    if (item !is Node) {
                        throw java.lang.IllegalArgumentException("`source` must be a node or list of nodes, but was: `${source}`")
                    }
                }
                else -> throw java.lang.IllegalArgumentException("`source` must be a node or list of nodes, but was: `${source}`")
            }
        }

        fun verifyNodeProperty(source: Any, nodeProperty: String) {
            when (source) {
                is Node -> verifyNodeProperty(source, nodeProperty)
                is List<*> -> source.forEach { node -> verifyNodeProperty(node as Node, nodeProperty) }
                else -> throw java.lang.IllegalArgumentException("`source` must be a node or list of nodes, but was: `${source}`")
            }
        }

        fun verifyKey(config: Map<String, Any>, property: String) {
            if (!config.containsKey(property)) {
                throw IllegalArgumentException("Missing parameter `$property`. An API key for the Amazon Comprehend API can be generated from https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/setup-credentials.html")
            }
        }

        private fun verifyNodeProperty(node: Node, nodeProperty: String) {
            if (!node.hasProperty(nodeProperty)) {
                throw IllegalArgumentException("$node does not have property `$nodeProperty`. Property can be configured using parameter `nodeProperty`.")
            }
        }

        fun convert(source: Any): List<Node> {
            return when (source) {
                is Node -> listOf(source)
                is List<*> -> source.map { item -> item as Node }
                else -> throw java.lang.IllegalArgumentException("`source` must be a node or list of nodes, but was: `${source}`")
            }
        }

        fun partition(nodes: List<Node>, size: Int): List<List<Node>> {
            if(size < 1) throw java.lang.IllegalArgumentException("size must be >= 1, but was:$size")

            var count = 0
            val result: MutableList<List<Node>> = mutableListOf()

            while(count < nodes.size) {
                result.add(nodes.subList(count, nodes.size.coerceAtMost(count + size)))
                count += size
            }

            return result
        }

    }
}