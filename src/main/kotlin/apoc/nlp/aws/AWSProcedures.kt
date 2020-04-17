package apoc.nlp.aws

import apoc.result.NodeWithMapResult
import apoc.util.JsonUtil
import com.amazonaws.services.comprehend.model.BatchDetectEntitiesItemResult
import com.amazonaws.services.comprehend.model.BatchItemError
import org.neo4j.graphdb.Node
import org.neo4j.logging.Log
import org.neo4j.procedure.*
import java.util.stream.Stream

class AWSProcedures {
    @Context
    @JvmField
    var log: Log? = null

    //    @Procedure(value = "apoc.ai.aws.sentiment", mode = Mode.READ)
//    @Description("Provides a sentiment analysis for provided text")
//    fun sentiment(@Name("apiKey") apiKey: String,
//                  @Name("apiSecret") apiSecret: String,
//                  @Name("data") data: Any,
//                  @Name(value = "config", defaultValue = "{}") config: Map<String, Any>)
//            : Stream<AIMapResult> = AWSClient(apiKey, apiSecret, log!!).sentiment(data, config).stream()
//
    @Procedure(value = "apoc.nlp.aws.entities.stream", mode = Mode.READ)
    @Description("Returns a stream of entities for provided text")
    fun entities(@Name("source") source: Any,
                 @Name(value = "config", defaultValue = "{}") config: Map<String, Any>)
            : Stream<NodeWithMapResult> {
        verifySource(source)
        val nodeProperty = getNodeProperty(config)
        verifyNodeProperty(source, nodeProperty)

        verifyKey(config, "key")
        verifyKey(config, "secret")

        val client = AWSClient(config, log!!)

        val detectEntitiesResult = client.entities(source, config)
        val resultList = detectEntitiesResult!!.resultList
        val errorList = detectEntitiesResult.errorList

        return convert(source).mapIndexed { index, node  -> transform(index, node, resultList, errorList) }.stream()
    }

    private fun transform(index: Int, node: Node, resultList: List<BatchDetectEntitiesItemResult>, errorList: List<BatchItemError>) : NodeWithMapResult {
        val result = resultList.find { result -> result.index == index }
        return if (result != null) {
            NodeWithMapResult.withResult(node, JsonUtil.OBJECT_MAPPER!!.convertValue(result, Map::class.java) as Map<String, Any?>)
        } else {
            val err = errorList.find { error -> error.index == index }
            NodeWithMapResult.withError(node, mapOf("code" to err?.errorCode, "message" to err?.errorMessage))
        }
    }

    private fun convert(source: Any) : List<Node> {
        return when (source) {
            is Node -> listOf(source)
            is List<*> -> source.map { item -> item as Node }
            else -> throw java.lang.IllegalArgumentException("`source` must be a node or list of nodes, but was: `${source}`")
        }
    }

    private fun verifySource(source: Any) {
        when (source) {
            is Node -> "ok"
            is List<*> -> source.forEach{ item -> if(item !is Node) {
                throw java.lang.IllegalArgumentException("`source` must be a node or list of nodes, but was: `${source}`")
            } }
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
//
//    @Procedure(value = "apoc.ai.aws.keyPhrases", mode = Mode.READ)
//    @Description("Provides a entity analysis for provided text")
//    fun keyPhrases(@Name("apiKey") apiKey: String,
//                   @Name("apiSecret") apiSecret: String,
//                   @Name("data") data: Any,
//                   @Name(value = "config", defaultValue = "{}") config: Map<String, Any>)
//            : Stream<AIMapResult> = AWSClient(apiKey, apiSecret, log!!).keyPhrases(data, config).stream()

//    @Procedure(value = "ai.aws.vision", mode = Mode.READ)
//    @Description("Provides a entity analysis for provided text")
//    fun vision(@Name("apiKey") apiKey: String,
//               @Name("apiSecret") apiSecret: String,
//               @Name("data") data: Any,
//               @Name(value = "config", defaultValue = "{}") config: Map<String, Any>): Stream<AIMapResult> = Stream.empty()

}