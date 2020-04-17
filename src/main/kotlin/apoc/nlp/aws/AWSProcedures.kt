package apoc.nlp.aws

import apoc.result.MapResult
import apoc.result.NodeWithMapResult
import org.neo4j.graphdb.Node
import org.neo4j.logging.Log
import org.neo4j.procedure.Context
import org.neo4j.procedure.Description
import org.neo4j.procedure.Mode
import org.neo4j.procedure.Name
import org.neo4j.procedure.Procedure
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
    @Description("Provides a entity analysis for provided text")
    fun entities(@Name("source") source: Any,
                 @Name(value = "config", defaultValue = "{}") config: Map<String, Any>)
            : Stream<NodeWithMapResult> {
        verifyKey(config, "key")
        verifyKey(config, "secret")

        val nodeProperty = getNodeProperty(config)
        verifyNodeProperty(source, nodeProperty)

        val client = AWSClient(config, log!!)

        val entities = client.entities(source, config)
        return entities.zip(convert(source)).map { result -> NodeWithMapResult(result.second, result.first) }.stream()
    }

    private fun convert(source: Any) : List<Node> {
        return when (source) {
            is Node -> listOf(source)
            is List<*> -> source.map { item -> item as Node }
            else -> throw java.lang.IllegalArgumentException("`source` must be a node or list of nodes, but was `${source::class.java.name}`")
        }
    }

    private fun verifyNodeProperty(source: Any, nodeProperty: String) {
        when (source) {
            is Node -> verifyNodeProperty(source, nodeProperty)
            is List<*> -> source.forEach { node -> verifyNodeProperty(node as Node, nodeProperty) }
            else -> throw java.lang.IllegalArgumentException("`source` must be a node or list of nodes, but was `${source::class.java.name}`")
        }
    }

    private fun verifyKey(config: Map<String, Any>, property: String) {
        if (!config.containsKey(property)) {
            throw IllegalArgumentException("Missing parameter `key`. An API key for the Amazon Comprehend API can be generated from https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/setup-credentials.html")
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