package apoc.nlp

import apoc.result.VirtualGraph
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.RelationshipType
import org.neo4j.graphdb.Transaction

abstract class NLPVirtualGraph {
    fun createAndStore(transaction: Transaction?): VirtualGraph {
        return createVirtualGraph(transaction)
    }

    fun create(): VirtualGraph {
        return createVirtualGraph(null)
    }

    abstract fun extractDocument(index: Int, sourceNode: Node) : Any?


    abstract fun createVirtualGraph(transaction: Transaction?): VirtualGraph
}