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

import org.neo4j.graphdb.Node
import org.neo4j.logging.Log

class DummyAzureClient(config: Map<String, Any>, private val log: Log) : AzureClient {
    private val nodeProperty = config.getOrDefault("nodeProperty", "text").toString()

    override fun entities(nodes: List<Node>, batchId: Int): List<Map<String, Any>> {
        val batchResults: MutableList<Map<String, Any>> = mutableListOf()

        nodes.map { node ->
            val nodeId = node.id
            val value = mapOf("id" to nodeId.toString(), "entities" to listOf(
                    mapOf(
                            "name" to  "token-1-node-${nodeId}-batch-${batchId}",
                            "type" to "Location",
                            "matches" to listOf(mapOf("entityTypeScore" to 0.2))
                    ),
                    mapOf(
                            "name" to  "token-2-node-${nodeId}-batch-${batchId}",
                            "type" to "DateTime",
                            "matches" to listOf(mapOf("entityTypeScore" to 0.1))
                    )
            ))
            batchResults += value
        }
        return batchResults
    }

    override fun sentiment(nodes: List<Node>, batchId: Int): List<Map<String, Any>> {
        val batchResults: MutableList<Map<String, Any>> = mutableListOf()

        nodes.map { node ->
            val nodeId = node.id
            val value = mapOf("id" to nodeId.toString(), "score" to 0.75)
            batchResults += value
        }
        return batchResults
    }

    override fun keyPhrases(nodes: List<Node>, batchId: Int): List<Map<String, Any>> {
        val batchResults: MutableList<Map<String, Any>> = mutableListOf()

        nodes.map { node ->
            val nodeId = node.id
            val value = mapOf("id" to nodeId.toString(), "keyPhrases" to listOf(
                    "keyPhrase-1-node-${nodeId}-batch-${batchId}",
                    "keyPhrase-2-node-${nodeId}-batch-${batchId}"
            ))
            batchResults += value
        }
        return batchResults

    }
}
