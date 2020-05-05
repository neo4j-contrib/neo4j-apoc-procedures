package apoc.nlp.gcp

import apoc.nlp.AWSVirtualEntitiesGraph
import apoc.nlp.GCPVirtualEntitiesGraph
import apoc.nlp.NLPHelperFunctions
import apoc.nlp.NLPHelperFunctions.Companion.convert
import apoc.nlp.NLPHelperFunctions.Companion.getNodeProperty
import apoc.nlp.NLPHelperFunctions.Companion.partition
import apoc.nlp.NLPHelperFunctions.Companion.verifyKey
import apoc.nlp.NLPHelperFunctions.Companion.verifyNodeProperty
import apoc.nlp.NLPHelperFunctions.Companion.verifySource
import apoc.result.*
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.RelationshipType
import org.neo4j.graphdb.Transaction
import org.neo4j.logging.Log
import org.neo4j.procedure.*
import java.util.stream.Stream

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
        NLPHelperFunctions.verifyKey(config, "key")

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
        NLPHelperFunctions.verifyKey(config, "key")

        val client = gcpClient(config)
        val relationshipType = NLPHelperFunctions.entityRelationshipType(config)
        val storeGraph: Boolean = config.getOrDefault("write", false) as Boolean

        val convertedSource = convert(source)
        return partition(convertedSource, 25)
                .mapIndexed { index, batch -> Pair(batch, client.entities(batch, index))  }
                .map { (batch, result) -> GCPVirtualEntitiesGraph(result, batch, relationshipType) }
                .map { graph -> if(storeGraph) graph.createAndStore(tx) else graph.create() }
                .stream()
    }

    @Procedure(value = "apoc.nlp.gcp.classify.stream", mode = Mode.READ)
    @Description("Classifies a document into categories.")
    fun classifyStream(@Name("sourceNode") sourceNode: Node,
                       @Name(value = "config", defaultValue = "{}") config: Map<String, Any>)
            : Stream<MapResult> {
        verifyKey(config, "key")
        val nodeProperty = getNodeProperty(config)
        verifyNodeProperty(sourceNode, nodeProperty)

//        return Stream.of(classify(config, sourceNode, nodeProperty))
        return Stream.of()
    }

    @Procedure(value = "apoc.nlp.gcp.classify.graph", mode = Mode.WRITE)
    @Description("Classifies a document into categories.")
    fun classifyGraph(@Name("sourceNode") sourceNode: Node,
                      @Name(value = "config", defaultValue = "{}") config: Map<String, Any>)
            : Stream<VirtualGraph> {
        verifyKey(config, "key")
        val nodeProperty = getNodeProperty(config)
        verifyNodeProperty(sourceNode, nodeProperty)

//        val response = classify(config, sourceNode, nodeProperty).value
//
//        val storeGraph:Boolean = config.getOrDefault("write", false) as Boolean
//        val graphConfig = mapOf(
//                "skipValidation" to true,
//                "mappings" to mapOf("$" to "Category{!name,type,@metadata}"),
//                "write" to storeGraph
//        )
//
//        val documentToGraph = DocumentToGraph(tx, GraphsConfig(graphConfig))
//        val graph = documentToGraph.create(response["categories"])
//
//        val mutableGraph = graph.graph.toMutableMap()
//
//        val nodes = (mutableGraph["nodes"] as Set<Node>).toMutableSet()
//        val relationships = (mutableGraph["relationships"] as Set<Relationship>).toMutableSet()
//        val node = if(storeGraph) {
//            mergeRelationships(tx!!, sourceNode, nodes, classifyRelationshipType(config)).forEach { rel -> relationships.add(rel) }
//            sourceNode
//        } else {
//            val virtualNode = VirtualNode(sourceNode, sourceNode.propertyKeys.toList())
//            createRelationships(virtualNode, nodes, classifyRelationshipType(config)).forEach { rel -> relationships.add(rel) }
//            virtualNode
//        }
//        nodes.add(node)

//        return Stream.of(VirtualGraph("Graph", nodes, relationships, emptyMap()))
        return Stream.of()
    }

//    @Procedure(value = "apoc.nlp.gcp.sentiment.stream", mode = Mode.READ)
//    @Description("Analyzes the sentiment of the provided text.")
//    fun sentimentStream(@Name("data") data: Any,
//                  @Name(value = "config", defaultValue = "{}") config: Map<String, Any>)
//            : Stream<AIMapResult> = Stream.of(GCPClient(config["key"].toString(), log!!).sentiment(data, config))


    private fun gcpClient(config: Map<String, Any>): GCPClient {
        val useDummyClient  = config.getOrDefault("unsupportedDummyClient", false) as Boolean
        return if (useDummyClient) DummyGCPClient(config, log!!) else RealGCPClient(config, log!!)
    }


    private fun classifyRelationshipType(config: Map<String, Any>) =
            RelationshipType.withName(config.getOrDefault("relationshipType", "CATEGORY").toString())



    private fun verifyNodeProperty(node: Node, nodeProperty: String) {
        if (!node.hasProperty(nodeProperty)) {
            throw IllegalArgumentException("$node does not have property `$nodeProperty`. Property can be configured using parameter `nodeProperty`.")
        }
    }

    private fun verifyKey(config: Map<String, Any>, property: String) {
        if (!config.containsKey(property)) {
            throw IllegalArgumentException("Missing parameter `key`. An API key for the Cloud Natural Language API can be generated from https://console.cloud.google.com/apis/credentials")
        }
    }


}

