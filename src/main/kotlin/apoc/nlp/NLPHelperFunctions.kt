package apoc.nlp

import apoc.util.Util
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.Relationship
import org.neo4j.graphdb.RelationshipType
import org.neo4j.graphdb.Transaction
import java.util.stream.Stream

class NLPHelperFunctions {
    companion object {
        fun createRelationships(node: Node, nodes: MutableSet<Node>, relationshipType: RelationshipType) =
                sequence {
                    for (n in nodes) {
                        yield(node.createRelationshipTo(n, relationshipType))
                    }
                }

        fun mergeRelationships(transaction: Transaction, node: Node, nodes: MutableSet<Node>, relType: RelationshipType): Stream<Relationship> {
            val cypher = """WITH ${'$'}startNode as startNode, ${'$'}endNodes as endNodes
            UNWIND endNodes AS endNode
            MERGE (startNode)-[r:${Util.quote(relType.name())}]->(endNode)
            RETURN r"""

            val params = mapOf("startNode" to node, "endNodes" to nodes)
            return transaction.execute(cypher, params).columnAs<Relationship>("r").stream()
        }

        fun entityRelationshipType(config: Map<String, Any>): RelationshipType = RelationshipType.withName(config.getOrDefault("relationshipType", "ENTITY").toString())
    }
}