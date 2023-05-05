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

import apoc.graph.document.builder.DocumentToGraph
import apoc.nlp.NLPHelperFunctions
import apoc.nlp.NLPVirtualGraph
import apoc.result.VirtualGraph
import apoc.result.VirtualNode
import org.neo4j.graphdb.*
import java.util.LinkedHashMap

data class AzureVirtualKeyPhrasesGraph(private val results: List<Map<String, Any>>, private val sourceNodes: List<Node>, val relType: RelationshipType): NLPVirtualGraph() {
    override fun extractDocument(index: Int, sourceNode: Node) : Any? = extractDocument(-1, sourceNode)
    private fun extractDocument(sourceNode: Node) : Any? = results.find { result -> result["id"] == sourceNode.id.toString()  }!!["keyPhrases"]

    companion object {
        val LABEL = Label { "KeyPhrase" }
    }

    override fun createVirtualGraph(transaction: Transaction?): VirtualGraph {
        val storeGraph = transaction != null

        val allNodes: MutableSet<Node> = mutableSetOf()
        val nonSourceNodes: MutableSet<Node> = mutableSetOf()
        val allRelationships: MutableSet<Relationship> = mutableSetOf()

        sourceNodes.forEach { sourceNode ->
            val document = extractDocument(sourceNode) as List<String>
            val virtualNodes = LinkedHashMap<MutableSet<String>, MutableSet<Node>>()
            val virtualNode = VirtualNode(sourceNode, sourceNode.propertyKeys.toList())

            val documentToNodes = DocumentToGraph.DocumentToNodes(nonSourceNodes, transaction)
            val entityNodes = mutableSetOf<Node>()
            val relationships = mutableSetOf<Relationship>()
            for (item in document) {
                val labels: Array<Label> = labels()
                val idValues: Map<String, Any> = mapOf("text" to item)

                val node = if (storeGraph) {
                    val entityNode = documentToNodes.getOrCreateRealNode(labels, idValues)
                    entityNode.setProperty("text", item)

                    entityNodes.add(entityNode)
                    relationships.add(NLPHelperFunctions.mergeRelationship(sourceNode, entityNode, relType))

                    sourceNode
                } else {
                    val entityNode = documentToNodes.getOrCreateVirtualNode(virtualNodes, labels, idValues)
                    entityNode.setProperty("text", item)
                    entityNodes.add(entityNode)

                    DocumentToGraph.getNodesWithSameLabels(virtualNodes, labels).add(entityNode)

                    relationships.add(NLPHelperFunctions.mergeRelationship(virtualNode, entityNode, relType))

                    virtualNode
                }
                allNodes.add(node)
            }

            nonSourceNodes.addAll(entityNodes)
            allNodes.addAll(entityNodes)
            allRelationships.addAll(relationships)
        }

        return VirtualGraph("Graph", allNodes, allRelationships, emptyMap())
    }

    private fun labels() = arrayOf(LABEL)

}