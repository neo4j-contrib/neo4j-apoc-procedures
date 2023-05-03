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
package apoc.nlp

import org.neo4j.graphdb.Direction
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.Relationship
import org.neo4j.graphdb.RelationshipType
import kotlin.streams.asStream

object NLPHelperFunctions {
    fun mergeRelationship(sourceNode: Node, nodesAndScore: Pair<Node, Number>, relationshipType: RelationshipType, relProperty: String): Relationship {
        val existingRelationships = sourceNode.getRelationships(Direction.OUTGOING, relationshipType).asSequence().asStream()
        val potentialRelationship = existingRelationships.filter { r -> r.endNode == nodesAndScore.first }.findFirst()

        return if(potentialRelationship.isPresent) {
            val relationship = potentialRelationship.get()
            if(nodesAndScore.second.toDouble() > (relationship.getProperty(relProperty) as Number).toDouble()) {
                relationship.setProperty(relProperty, nodesAndScore.second)
            }
            relationship
        } else {
            val relationship = sourceNode.createRelationshipTo(nodesAndScore.first, relationshipType)
            relationship.setProperty(relProperty, nodesAndScore.second)
            relationship
        }
    }

    fun mergeRelationship(sourceNode: Node, targetNode: Node, relationshipType: RelationshipType): Relationship {
        val existingRelationships = sourceNode.getRelationships(Direction.OUTGOING, relationshipType).asSequence().asStream()
        val potentialRelationship = existingRelationships.filter { r -> r.endNode == targetNode }.findFirst()

        return if (potentialRelationship.isPresent) {
            val relationship = potentialRelationship.get()
            relationship
        } else {
            val relationship = sourceNode.createRelationshipTo(targetNode, relationshipType)
            relationship
        }
    }

    fun entityRelationshipType(config: Map<String, Any>): RelationshipType {
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
            throw IllegalArgumentException("Missing parameter `$property`.")
        }
    }

    fun verifyKeys(config: Map<String, Any>, vararg properties: String) {
        properties.forEach { verifyKey(config, it) }
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