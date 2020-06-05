package apoc.nlp.aws

import apoc.Extended
import apoc.nlp.NLPHelperFunctions
import apoc.nlp.NLPHelperFunctions.getNodeProperty
import apoc.nlp.NLPHelperFunctions.keyPhraseRelationshipType
import apoc.nlp.NLPHelperFunctions.partition
import apoc.nlp.NLPHelperFunctions.verifyKey
import apoc.nlp.NLPHelperFunctions.verifyNodeProperty
import apoc.nlp.NLPHelperFunctions.verifySource
import apoc.result.NodeWithMapResult
import apoc.result.VirtualGraph
import apoc.util.JsonUtil
import com.amazonaws.services.comprehend.model.BatchDetectEntitiesResult
import com.amazonaws.services.comprehend.model.BatchDetectKeyPhrasesResult
import com.amazonaws.services.comprehend.model.BatchDetectSentimentResult
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.Transaction
import org.neo4j.logging.Log
import org.neo4j.procedure.Context
import org.neo4j.procedure.Description
import org.neo4j.procedure.Mode
import org.neo4j.procedure.Name
import org.neo4j.procedure.Procedure
import java.util.stream.Stream

@Extended
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

        val convertedSource = NLPHelperFunctions.convert(source)
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
        val relationshipProperty = config.getOrDefault("writeRelationshipProperty", "score") as String
        val storeGraph: Boolean = config.getOrDefault("write", false) as Boolean
        val scoreCutoff = config.getOrDefault("scoreCutoff", 0.0) as Number

        val convertedSource = NLPHelperFunctions.convert(source)

        return partition(convertedSource, 25)
                .mapIndexed { index, batch -> Pair(batch, client.entities(batch, index))  }
                .map { (batch, result) -> AWSVirtualEntitiesGraph(result!!, batch, relationshipType, relationshipProperty, scoreCutoff) }
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

        val convertedSource = NLPHelperFunctions.convert(source)

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
        val relationshipType = keyPhraseRelationshipType(config)
        val relationshipProperty = config.getOrDefault("writeRelationshipProperty", "score") as String
        val storeGraph: Boolean = config.getOrDefault("write", false) as Boolean
        val scoreCutoff = config.getOrDefault("scoreCutoff", 0.0) as Number

        val convertedSource = NLPHelperFunctions.convert(source)

        return partition(convertedSource, 25)
                .mapIndexed { index, batch -> Pair(batch, client.keyPhrases(batch, index))  }
                .map { (batch, result) -> AWSVirtualKeyPhrasesGraph(result!!, batch, relationshipType, relationshipProperty, scoreCutoff) }
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

        val convertedSource = NLPHelperFunctions.convert(source)

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

        val convertedSource = NLPHelperFunctions.convert(source)

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




    }

}