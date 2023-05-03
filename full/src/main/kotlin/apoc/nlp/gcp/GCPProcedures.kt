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

import apoc.Extended
import apoc.nlp.NLPHelperFunctions
import apoc.nlp.NLPHelperFunctions.convert
import apoc.nlp.NLPHelperFunctions.getNodeProperty
import apoc.nlp.NLPHelperFunctions.partition
import apoc.nlp.NLPHelperFunctions.verifyKey
import apoc.nlp.NLPHelperFunctions.verifyNodeProperty
import apoc.nlp.NLPHelperFunctions.verifySource
import apoc.result.NodeValueErrorMapResult
import apoc.result.VirtualGraph
import org.neo4j.graphdb.Transaction
import org.neo4j.logging.Log
import org.neo4j.procedure.*
import java.util.stream.Stream

@Extended
class GCPProcedures {
    @Context
    @JvmField
    var log: Log? = null

    @Context
    @JvmField
    var tx: Transaction? = null

    @Procedure(value = "apoc.nlp.gcp.entities.stream", mode = Mode.READ)
    @Description("Returns a stream of entities for provided text")
    fun entitiesStream(@Name("source") source: Any,
                       @Name(value = "config", defaultValue = "{}") config: Map<String, Any>)
            : Stream<NodeValueErrorMapResult> {
        verifySource(source)
        val nodeProperty = getNodeProperty(config)
        verifyNodeProperty(source, nodeProperty)
        verifyKey(config, "key")

        val client = gcpClient(config)

        val convertedSource = convert(source)
        val batches = partition(convertedSource, 25)
        return batches.mapIndexed { index, batch -> Pair(batch, client.entities(batch, index)) }.stream()
                .flatMap { (_, result) ->  result.map { it }.stream() }
    }

    @Procedure(value = "apoc.nlp.gcp.entities.graph", mode = Mode.WRITE)
    @Description("Creates a (virtual) entity graph for provided text")
    fun entitiesGraph(@Name("source") source: Any,
                      @Name(value = "config", defaultValue = "{}") config: Map<String, Any>)
            : Stream<VirtualGraph> {
        verifySource(source)
        val nodeProperty = getNodeProperty(config)
        verifyNodeProperty(source, nodeProperty)
        verifyKey(config, "key")

        val client = gcpClient(config)
        val relationshipType = NLPHelperFunctions.entityRelationshipType(config)
        val relationshipProperty = config.getOrDefault("writeRelationshipProperty", "score") as String
        val storeGraph: Boolean = config.getOrDefault("write", false) as Boolean
        val scoreCutoff = config.getOrDefault("scoreCutoff", 0.0) as Number

        val convertedSource = convert(source)
        return partition(convertedSource, 25)
                .mapIndexed { index, batch -> Pair(batch, client.entities(batch, index))  }
                .map { (batch, result) -> GCPVirtualEntitiesGraph(result, batch, relationshipType, relationshipProperty, scoreCutoff) }
                .map { graph -> if(storeGraph) graph.createAndStore(tx) else graph.create() }
                .stream()
    }

    @Procedure(value = "apoc.nlp.gcp.classify.stream", mode = Mode.READ)
    @Description("Classifies a document into categories.")
    fun classifyStream(@Name("source") source: Any,
                       @Name(value = "config", defaultValue = "{}") config: Map<String, Any>)
            : Stream<NodeValueErrorMapResult> {
        verifySource(source)
        val nodeProperty = getNodeProperty(config)
        verifyNodeProperty(source, nodeProperty)
        verifyKey(config, "key")

        val client = gcpClient(config)

        val convertedSource = convert(source)
        val batches = partition(convertedSource, 25)
        return batches.mapIndexed { index, batch -> Pair(batch, client.classify(batch, index)) }.stream()
                .flatMap { (_, result) ->  result.map { it }.stream() }
    }

    @Procedure(value = "apoc.nlp.gcp.classify.graph", mode = Mode.WRITE)
    @Description("Classifies a document into categories.")
    fun classifyGraph(@Name("source") source: Any,
                      @Name(value = "config", defaultValue = "{}") config: Map<String, Any>)
            : Stream<VirtualGraph> {
        verifySource(source)
        val nodeProperty = getNodeProperty(config)
        verifyNodeProperty(source, nodeProperty)
        verifyKey(config, "key")

        val client = gcpClient(config)
        val relationshipType = NLPHelperFunctions.categoryRelationshipType(config)
        val relationshipProperty = config.getOrDefault("writeRelationshipProperty", "score") as String
        val storeGraph: Boolean = config.getOrDefault("write", false) as Boolean
        val scoreCutoff = config.getOrDefault("scoreCutoff", 0.0) as Number

        val convertedSource = convert(source)
        return partition(convertedSource, 25)
                .mapIndexed { index, batch -> Pair(batch, client.classify(batch, index))  }
                .map { (batch, result) -> GCPVirtualClassificationGraph(result, batch, relationshipType,relationshipProperty, scoreCutoff) }
                .map { graph -> if(storeGraph) graph.createAndStore(tx) else graph.create() }
                .stream()
    }


    private fun gcpClient(config: Map<String, Any>): GCPClient {
        val useDummyClient  = config.getOrDefault("unsupportedDummyClient", false) as Boolean
        return if (useDummyClient) DummyGCPClient(config, log!!) else RealGCPClient(config, log!!)
    }

}