package apoc.nlp.aws

import apoc.ai.service.AWSClient
import apoc.nlp.NLPHelperFunctions
import apoc.nlp.VirtualNLPGraph
import apoc.result.NodeWithMapResult
import apoc.result.VirtualGraph
import apoc.util.JsonUtil
import com.amazonaws.services.comprehend.model.BatchDetectEntitiesResult
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

        val client: AWSClient = RealAWSClient(config, log!!)
        val detectEntitiesResult = client.entities(source)

        return convert(source).mapIndexed { index, node -> transformResults(index, node, detectEntitiesResult!!) }.stream()
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

        val client = RealAWSClient(config, log!!)
        val detectEntitiesResult = client.entities(source)

        val storeGraph: Boolean = config.getOrDefault("write", false) as Boolean

        val relationshipType = NLPHelperFunctions.entityRelationshipType(config)

        val virtualNLPGraph = VirtualNLPGraph(detectEntitiesResult!!, convert(source), relationshipType, ENTITY_MAPPING)
        return if (storeGraph) {
            Stream.of(virtualNLPGraph.createAndStore(tx))
        } else {
            Stream.of(virtualNLPGraph.create())
        }
    }

    companion object {
        val ENTITY_MAPPING = mapOf("$" to "Entity{!text,type,@metadata}")

        fun transformResults(index: Int, node: Node, res: BatchDetectEntitiesResult): NodeWithMapResult {
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
    }

}