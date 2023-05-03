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


import apoc.Extended
import apoc.nlp.NLPHelperFunctions
import apoc.nlp.NLPHelperFunctions.convert
import apoc.nlp.NLPHelperFunctions.getNodeProperty
import apoc.nlp.NLPHelperFunctions.verifyKeys
import apoc.nlp.NLPHelperFunctions.verifyNodeProperty
import apoc.nlp.NLPHelperFunctions.verifySource
import apoc.result.NodeWithMapResult
import apoc.result.VirtualGraph
import org.neo4j.graphdb.Transaction
import org.neo4j.logging.Log
import org.neo4j.procedure.*
import java.util.stream.Stream

@Extended
class AzureProcedures {
    @Context
    @JvmField
    var log: Log? = null

    @Context
    @JvmField
    var tx: Transaction? = null

    companion object {
        val CONFIG_PROPS = listOf("key", "url")
    }

    @Procedure(value = "apoc.nlp.azure.sentiment.stream", mode = Mode.READ)
    @Description("Provides a sentiment analysis for provided text")
    fun sentiment(@Name("source") source: Any,
                  @Name(value = "config", defaultValue = "{}") config: Map<String, Any>)
            : Stream<NodeWithMapResult> {
        verifySource(source)
        val nodeProperty = getNodeProperty(config)
        verifyNodeProperty(source, nodeProperty)
        verifyKeys(config, *CONFIG_PROPS.toTypedArray())

        val client = azureClient(config)
        val convertedSource = convert(source)
        val batches = NLPHelperFunctions.partition(convertedSource, 25)

        return batches.mapIndexed {index, batch -> client.sentiment(batch, index) }.stream()
                .flatMap { result -> result.map { RealAzureClient.responseToNodeWithMapResult(it, convertedSource) }.stream() }
    }

    @Procedure(value = "apoc.nlp.azure.sentiment.graph", mode = Mode.WRITE)
    @Description("Creates a (virtual) sentiment graph for provided text")
    fun sentimentGraph(@Name("source") source: Any,
                        @Name(value = "config", defaultValue = "{}") config: Map<String, Any>) : Stream<VirtualGraph> {
        verifySource(source)
        val nodeProperty = getNodeProperty(config)
        verifyNodeProperty(source, nodeProperty)
        verifyKeys(config, *CONFIG_PROPS.toTypedArray())

        val client = azureClient(config)
        val relationshipType = NLPHelperFunctions.keyPhraseRelationshipType(config)
        val storeGraph: Boolean = config.getOrDefault("write", false) as Boolean

        val convertedSource = NLPHelperFunctions.convert(source)

        return NLPHelperFunctions.partition(convertedSource, 25)
                .mapIndexed { index, batch -> Pair(batch, client.sentiment(batch, index))  }
                .map { (batch, result) -> AzureVirtualSentimentVirtualGraph(result, batch) }
                .map { graph -> if(storeGraph) graph.createAndStore(tx) else graph.create() }
                .stream()
    }

    @Procedure(value = "apoc.nlp.azure.entities.stream", mode = Mode.READ)
    @Description("Provides a entity analysis for provided text")
    fun entities(@Name("source") source: Any,
                 @Name(value = "config", defaultValue = "{}") config: Map<String, Any>)
            : Stream<NodeWithMapResult> {
        verifySource(source)
        val nodeProperty = getNodeProperty(config)
        verifyNodeProperty(source, nodeProperty)
        verifyKeys(config, *CONFIG_PROPS.toTypedArray())

        val client = azureClient(config)
        val convertedSource = convert(source)
        val batches = NLPHelperFunctions.partition(convertedSource, 25)

        return batches.mapIndexed {index, batch -> client.entities(batch, index) }.stream()
                .flatMap { result -> result.map { RealAzureClient.responseToNodeWithMapResult(it, convertedSource) }.stream() }

    }

    @Procedure(value = "apoc.nlp.azure.entities.graph", mode = Mode.WRITE)
    @Description("Creates a (virtual) entity graph for provided text")
    fun entitiesGraph(@Name("source") source: Any,
                      @Name(value = "config", defaultValue = "{}") config: Map<String, Any>) : Stream<VirtualGraph> {
        verifySource(source)
        val nodeProperty = getNodeProperty(config)
        verifyNodeProperty(source, nodeProperty)
        verifyKeys(config, *CONFIG_PROPS.toTypedArray())

        val client = azureClient(config)
        val relationshipType = NLPHelperFunctions.entityRelationshipType(config)
        val relationshipProperty = config.getOrDefault("writeRelationshipProperty", "score") as String
        val storeGraph: Boolean = config.getOrDefault("write", false) as Boolean
        val scoreCutoff = config.getOrDefault("scoreCutoff", 0.0) as Number

        val convertedSource = NLPHelperFunctions.convert(source)

        return NLPHelperFunctions.partition(convertedSource, 25)
                .mapIndexed { index, batch -> Pair(batch, client.entities(batch, index))  }
                .map { (batch, result) -> AzureVirtualEntitiesGraph(result, batch, relationshipType, relationshipProperty, scoreCutoff) }
                .map { graph -> if(storeGraph) graph.createAndStore(tx) else graph.create() }
                .stream()
    }

    @Procedure(value = "apoc.nlp.azure.keyPhrases.stream", mode = Mode.READ)
    @Description("Provides a entity analysis for provided text")
    fun keyPhrases(@Name("source") source: Any,
                   @Name(value = "config", defaultValue = "{}") config: Map<String, Any>)
            : Stream<NodeWithMapResult> {
        verifySource(source)
        val nodeProperty = getNodeProperty(config)
        verifyNodeProperty(source, nodeProperty)
        verifyKeys(config, *CONFIG_PROPS.toTypedArray())

        val client = azureClient(config)
        val convertedSource = convert(source)
        val batches = NLPHelperFunctions.partition(convertedSource, 25)

        return batches.mapIndexed { index, batch -> client.keyPhrases(batch, index) }.stream()
                .flatMap { result -> result.map { RealAzureClient.responseToNodeWithMapResult(it, convertedSource) }.stream() }
    }

    @Procedure(value = "apoc.nlp.azure.keyPhrases.graph", mode = Mode.WRITE)
    @Description("Creates a (virtual) key phrase graph for provided text")
    fun keyPhrasesGraph(@Name("source") source: Any,
                      @Name(value = "config", defaultValue = "{}") config: Map<String, Any>) : Stream<VirtualGraph> {
        verifySource(source)
        val nodeProperty = getNodeProperty(config)
        verifyNodeProperty(source, nodeProperty)
        verifyKeys(config, *CONFIG_PROPS.toTypedArray())

        val client = azureClient(config)
        val relationshipType = NLPHelperFunctions.keyPhraseRelationshipType(config)
        val storeGraph: Boolean = config.getOrDefault("write", false) as Boolean

        val convertedSource = NLPHelperFunctions.convert(source)

        return NLPHelperFunctions.partition(convertedSource, 25)
                .mapIndexed { index, batch -> Pair(batch, client.keyPhrases(batch, index))  }
                .map { (batch, result) -> AzureVirtualKeyPhrasesGraph(result, batch, relationshipType) }
                .map { graph -> if(storeGraph) graph.createAndStore(tx) else graph.create() }
                .stream()
    }

    private fun azureClient(config: Map<String, Any>): AzureClient {
        val useDummyClient  = config.getOrDefault("unsupportedDummyClient", false) as Boolean
        return if (useDummyClient) DummyAzureClient(config, log!!)
        else RealAzureClient(config.getValue("url").toString(), config.getValue("key").toString(), log!!, config)
    }

//    @Procedure(value = "apoc.nlp.azure.vision.stream", mode = Mode.READ)
//    @Description("Provides a entity analysis for provided text")
//    fun vision(@Name("url") url: String,
//               @Name("key") key: String,
//               @Name("data") data: Any,
//               @Name(value = "config", defaultValue = "{}") config: Map<String, Any>)
//            : Stream<NodeWithMapResult> = AzureClient(url, key, log!!).vision(data, config).stream()

}