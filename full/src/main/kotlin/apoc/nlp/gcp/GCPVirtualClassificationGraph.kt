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
package apoc.nlp.gcp

import apoc.graph.document.builder.DocumentToGraph
import apoc.nlp.NLPHelperFunctions
import apoc.nlp.NLPVirtualGraph
import apoc.result.NodeValueErrorMapResult
import apoc.result.VirtualGraph
import apoc.result.VirtualNode
import org.apache.commons.text.WordUtils
import org.neo4j.graphdb.Label
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.Relationship
import org.neo4j.graphdb.RelationshipType
import org.neo4j.graphdb.Transaction
import java.util.LinkedHashMap

data class GCPVirtualClassificationGraph(private val results: List<NodeValueErrorMapResult>, private val sourceNodes: List<Node>, val relType: RelationshipType, val relProperty: String, val cutoff: Number): NLPVirtualGraph() {
    override fun extractDocument(index: Int, sourceNode: Node) : Any? = results[index].value["categories"]

    companion object {
        const val SCORE_PROPERTY = "confidence"
        val LABEL = Label { "Category" }
        val KEY_MAPPINGS = mapOf("name" to "text")
        val ID_MAPPINGS = mapOf("name" to "text")
    }

    override fun createVirtualGraph(transaction: Transaction?): VirtualGraph {
        val storeGraph = transaction != null

        val allNodes: MutableSet<Node> = mutableSetOf()
        val nonSourceNodes: MutableSet<Node> = mutableSetOf()
        val allRelationships: MutableSet<Relationship> = mutableSetOf()

        sourceNodes.forEachIndexed { index, sourceNode ->
            val document = extractDocument(index, sourceNode) as List<Map<String, Any>>
            val virtualNodes = LinkedHashMap<MutableSet<String>, MutableSet<Node>>()
            val virtualNode = VirtualNode(sourceNode, sourceNode.propertyKeys.toList())

            val documentToNodes = DocumentToGraph.DocumentToNodes(nonSourceNodes, transaction)
            val entityNodes = mutableSetOf<Node>()
            val relationships = mutableSetOf<Relationship>()
            for (item in document) {
                val labels: Array<Label> = arrayOf(LABEL)
                val idValues: Map<String, Any> = ID_MAPPINGS.map { (key, value) -> value to item[key].toString() }.toMap()

                val score = item[SCORE_PROPERTY] as Number
                if (score.toDouble() >= cutoff.toDouble()) {
                    val node = if (storeGraph) {
                        val entityNode = documentToNodes.getOrCreateRealNode(labels, idValues)
                        setProperties(entityNode, item)
                        entityNodes.add(entityNode)

                        val nodeAndScore = Pair(entityNode, score)
                        relationships.add(NLPHelperFunctions.mergeRelationship(sourceNode, nodeAndScore, relType, relProperty))

                        sourceNode
                    } else {
                        val entityNode = documentToNodes.getOrCreateVirtualNode(virtualNodes, labels, idValues)
                        setProperties(entityNode, item)
                        entityNodes.add(entityNode)

                        DocumentToGraph.getNodesWithSameLabels(virtualNodes, labels).add(entityNode)

                        val nodeAndScore = Pair(entityNode, score)
                        relationships.add(NLPHelperFunctions.mergeRelationship(virtualNode, nodeAndScore, relType, relProperty))

                        virtualNode
                    }
                    allNodes.add(node)
                }
            }

            nonSourceNodes.addAll(entityNodes)
            allNodes.addAll(entityNodes)
            allRelationships.addAll(relationships)
        }

        return VirtualGraph("Graph", allNodes, allRelationships, emptyMap())
    }

    private fun asLabel(value: String) : String {
        return WordUtils.capitalizeFully(value, '_', ' ')
                .replace("_".toRegex(), "")
                .replace(" ".toRegex(), "")
    }

    private fun setProperties(entityNode: Node, item: Map<String, Any>) {
        KEY_MAPPINGS.forEach { (key, value) ->
            entityNode.setProperty(value, item[key].toString())
        }
    }
}