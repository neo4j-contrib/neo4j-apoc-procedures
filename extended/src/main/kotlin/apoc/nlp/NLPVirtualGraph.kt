package apoc.nlp

import apoc.result.VirtualGraphExtended
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.Transaction

abstract class NLPVirtualGraph {
    fun createAndStore(transaction: Transaction?): VirtualGraphExtended {
        return createVirtualGraph(transaction)
    }

    fun create(): VirtualGraphExtended {
        return createVirtualGraph(null)
    }

    abstract fun extractDocument(index: Int, sourceNode: Node) : Any?


    abstract fun createVirtualGraph(transaction: Transaction?): VirtualGraphExtended
}