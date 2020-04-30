package apoc.nlp.aws

import apoc.ai.service.AWSClient
import apoc.nlp.AWSVirtualEntitiesGraph
import apoc.nlp.AWSVirtualKeyPhrasesGraph
import apoc.nlp.AWSVirtualSentimentVirtualGraph
import apoc.nlp.NLPHelperFunctions
import apoc.result.NodeWithMapResult
import apoc.result.VirtualGraph
import apoc.util.JsonUtil
import com.amazonaws.services.comprehend.model.BatchDetectEntitiesResult
import com.amazonaws.services.comprehend.model.BatchDetectKeyPhrasesResult
import com.amazonaws.services.comprehend.model.BatchDetectSentimentResult
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.Transaction
import org.neo4j.logging.Log
import org.neo4j.procedure.*
import java.util.stream.Stream

class AWSProcedures {
    @Context
    @JvmField
    var log: Log? = null

    @Context
    @JvmField
    var tx: Transaction? = null

    @Procedure(value = "apoc.nlp.aws.entities.stream", mode = Mode.READ)
    @Description("Returns a stream of entities for provided text")
    fun entitiesStream(@Name("source") source: Any,
                 @Name(value = "config", defaultValue = "{}") config: Map<String, Any>) : Stream<NodeWithMapResult> {
        verifySource(source)
        val nodeProperty = getNodeProperty(config)
        verifyNodeProperty(source, nodeProperty)
        verifyKey(config, "key")
        verifyKey(config, "secret")

        val client: AWSClient = awsClient(config)

        val convertedSource = convert(source)

        val batches = partition(convertedSource, 25)

        return batches.mapIndexed { index, batch -> Pair(batch, client.entities(batch, index)) }.stream()
                .flatMap { (batch, result) ->
                    batch.mapIndexed { index, node  -> transformResults(index, node, result!!) }.stream() }
    }

    @Procedure(value = "apoc.nlp.aws.entities.graph", mode = Mode.WRITE)
    @Description("Creates a (virtual) entity graph for provided text")
    fun entitiesGraph(@Name("source") source: Any,
                      @Name(value = "config", defaultValue = "{}") config: Map<String, Any>) : Stream<VirtualGraph> {
        verifySource(source)
        val nodeProperty = getNodeProperty(config)
        verifyNodeProperty(source, nodeProperty)
        verifyKey(config, "key")
        verifyKey(config, "secret")

        val client = awsClient(config)
        val relationshipType = NLPHelperFunctions.entityRelationshipType(config)
        val storeGraph: Boolean = config.getOrDefault("write", false) as Boolean

        val convertedSource = convert(source)

        return partition(convertedSource, 25)
                .mapIndexed { index, batch -> Pair(batch, client.entities(batch, index))  }
                .map { (batch, result) -> AWSVirtualEntitiesGraph(result!!, batch, relationshipType) }
                .map { graph -> if(storeGraph) graph.createAndStore(tx) else graph.create() }
                .stream()
    }

    @Procedure(value = "apoc.nlp.aws.keyPhrases.stream", mode = Mode.READ)
    @Description("Returns a stream of key phrases for provided text")
    fun keyPhrasesStream(@Name("source") source: Any,
                       @Name(value = "config", defaultValue = "{}") config: Map<String, Any>) : Stream<NodeWithMapResult> {
        verifySource(source)
        val nodeProperty = getNodeProperty(config)
        verifyNodeProperty(source, nodeProperty)
        verifyKey(config, "key")
        verifyKey(config, "secret")

        val client: AWSClient = awsClient(config)

        val convertedSource = convert(source)

        val batches = partition(convertedSource, 25)

        return batches.mapIndexed { index, batch -> Pair(batch, client.keyPhrases(batch, index)) }.stream()
                .flatMap { (batch, result) ->
                    batch.mapIndexed { index, node  -> transformResults(index, node, result!!) }.stream() }
    }

    @Procedure(value = "apoc.nlp.aws.keyPhrases.graph", mode = Mode.WRITE)
    @Description("Creates a (virtual) key phrases graph for provided text")
    fun keyPhrasesGraph(@Name("source") source: Any,
                      @Name(value = "config", defaultValue = "{}") config: Map<String, Any>) : Stream<VirtualGraph> {
        verifySource(source)
        val nodeProperty = getNodeProperty(config)
        verifyNodeProperty(source, nodeProperty)
        verifyKey(config, "key")
        verifyKey(config, "secret")

        val client = awsClient(config)
        val relationshipType = NLPHelperFunctions.keyPhraseRelationshipType(config)
        val storeGraph: Boolean = config.getOrDefault("write", false) as Boolean

        val convertedSource = convert(source)

        return partition(convertedSource, 25)
                .mapIndexed { index, batch -> Pair(batch, client.keyPhrases(batch, index))  }
                .map { (batch, result) -> AWSVirtualKeyPhrasesGraph(result!!, batch, relationshipType) }
                .map { graph -> if(storeGraph) graph.createAndStore(tx) else graph.create() }
                .stream()
    }

    @Procedure(value = "apoc.nlp.aws.sentiment.stream", mode = Mode.READ)
    @Description("Returns stream of sentiment for items in provided text")
    fun sentimentStream(@Name("source") source: Any,
                         @Name(value = "config", defaultValue = "{}") config: Map<String, Any>) : Stream<NodeWithMapResult> {
        verifySource(source)
        val nodeProperty = getNodeProperty(config)
        verifyNodeProperty(source, nodeProperty)
        verifyKey(config, "key")
        verifyKey(config, "secret")

        val client: AWSClient = awsClient(config)

        val convertedSource = convert(source)

        val batches = partition(convertedSource, 25)

        return batches.mapIndexed { index, batch -> Pair(batch, client.sentiment(batch, index)) }.stream()
                .flatMap { (batch, result) ->
                    batch.mapIndexed { index, node  -> transformResults(index, node, result!!) }.stream() }
    }

    @Procedure(value = "apoc.nlp.aws.sentiment.graph", mode = Mode.WRITE)
    @Description("Creates a (virtual) sentiment graph for provided text")
    fun sentimentGraph(@Name("source") source: Any,
                        @Name(value = "config", defaultValue = "{}") config: Map<String, Any>) : Stream<VirtualGraph> {
        verifySource(source)
        val nodeProperty = getNodeProperty(config)
        verifyNodeProperty(source, nodeProperty)
        verifyKey(config, "key")
        verifyKey(config, "secret")

        val client = awsClient(config)
        val storeGraph: Boolean = config.getOrDefault("write", false) as Boolean

        val convertedSource = convert(source)

        return partition(convertedSource, 25)
                .mapIndexed { index, batch -> Pair(batch, client.sentiment(batch, index))  }
                .map { (batch, result) -> AWSVirtualSentimentVirtualGraph(result!!, batch) }
                .map { graph -> if(storeGraph) graph.createAndStore(tx) else graph.create() }
                .stream()
    }

    private fun awsClient(config: Map<String, Any>): AWSClient {
        val useDummyClient  = config.getOrDefault("unsupportedDummyClient", false) as Boolean
        return if (useDummyClient) DummyAWSClient(config, log!!) else RealAWSClient(config, log!!)
    }

    companion object {
        fun transformResults(index: Int, node: Node, res: BatchDetectEntitiesResult): NodeWithMapResult {
            val result = res.resultList.find { result -> result.index == index }
            return if (result != null) {
                NodeWithMapResult.withResult(node, JsonUtil.OBJECT_MAPPER!!.convertValue(result, Map::class.java) as Map<String, Any?>)
            } else {
                val err = res.errorList.find { error -> error.index == index }
                NodeWithMapResult.withError(node, mapOf("code" to err?.errorCode, "message" to err?.errorMessage))
            }
        }

        fun transformResults(index: Int, node: Node, res: BatchDetectKeyPhrasesResult): NodeWithMapResult {
            val result = res.resultList.find { result -> result.index == index }
            return if (result != null) {
                NodeWithMapResult.withResult(node, JsonUtil.OBJECT_MAPPER!!.convertValue(result, Map::class.java) as Map<String, Any?>)
            } else {
                val err = res.errorList.find { error -> error.index == index }
                NodeWithMapResult.withError(node, mapOf("code" to err?.errorCode, "message" to err?.errorMessage))
            }
        }

        fun transformResults(index: Int, node: Node, res: BatchDetectSentimentResult): NodeWithMapResult {
            val result = res.resultList.find { result -> result.index == index }
            return if (result != null) {
                NodeWithMapResult.withResult(node, JsonUtil.OBJECT_MAPPER!!.convertValue(result, Map::class.java) as Map<String, Any?>)
            } else {
                val err = res.errorList.find { error -> error.index == index }
                NodeWithMapResult.withError(node, mapOf("code" to err?.errorCode, "message" to err?.errorMessage))
            }
        }

        private fun convert(source: Any): List<Node> {
            return when (source) {
                is Node -> listOf(source)
                is List<*> -> source.map { item -> item as Node }
                else -> throw java.lang.IllegalArgumentException("`source` must be a node or list of nodes, but was: `${source}`")
            }
        }

        private fun verifySource(source: Any) {
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

        private fun verifyNodeProperty(source: Any, nodeProperty: String) {
            when (source) {
                is Node -> verifyNodeProperty(source, nodeProperty)
                is List<*> -> source.forEach { node -> verifyNodeProperty(node as Node, nodeProperty) }
                else -> throw java.lang.IllegalArgumentException("`source` must be a node or list of nodes, but was: `${source}`")
            }
        }

        private fun verifyKey(config: Map<String, Any>, property: String) {
            if (!config.containsKey(property)) {
                throw IllegalArgumentException("Missing parameter `$property`. An API key for the Amazon Comprehend API can be generated from https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/setup-credentials.html")
            }
        }

        private fun getNodeProperty(config: Map<String, Any>): String {
            return config.getOrDefault("nodeProperty", "text").toString()
        }

        private fun verifyNodeProperty(node: Node, nodeProperty: String) {
            if (!node.hasProperty(nodeProperty)) {
                throw IllegalArgumentException("$node does not have property `$nodeProperty`. Property can be configured using parameter `nodeProperty`.")
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

}