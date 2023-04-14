/**
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package apoc.nlp.azure

import apoc.nlp.NLPVirtualGraph
import apoc.result.VirtualGraph
import apoc.result.VirtualNode
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.Relationship
import org.neo4j.graphdb.Transaction

data class AzureVirtualSentimentVirtualGraph(private val results: List<Map<String, Any>>, private val sourceNodes: List<Node>): NLPVirtualGraph() {
    override fun extractDocument(index: Int, sourceNode: Node) : Any? = extractDocument(-1, sourceNode)
    private fun extractDocument(sourceNode: Node) : Any? = results.find { result -> result["id"] == sourceNode.id.toString()  }

    override fun createVirtualGraph(transaction: Transaction?): VirtualGraph {
        val storeGraph = transaction != null

        val allNodes: MutableSet<Node> = mutableSetOf()
        val allRelationships: MutableSet<Relationship> = mutableSetOf()

        sourceNodes.forEach { sourceNode ->
            val document = extractDocument(sourceNode) as Map<String, Any>
            val score = document["score"] as Number

            val node = if (storeGraph) {
                sourceNode.setProperty("sentimentScore", score)
                sourceNode
            } else {
                val virtualNode = VirtualNode(sourceNode, sourceNode.propertyKeys.toList())
                virtualNode.setProperty("sentimentScore", score)
                virtualNode
            }

            allNodes.add(node)
        }

        return VirtualGraph("Graph", allNodes, allRelationships, emptyMap())
    }
}