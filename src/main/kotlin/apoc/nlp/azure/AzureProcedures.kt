package apoc.nlp.azure


import apoc.nlp.NLPHelperFunctions.convert
import apoc.nlp.NLPHelperFunctions.getNodeProperty
import apoc.nlp.NLPHelperFunctions.verifyKeys
import apoc.nlp.NLPHelperFunctions.verifyNodeProperty
import apoc.nlp.NLPHelperFunctions.verifySource
import apoc.result.NodeWithMapResult
import org.neo4j.logging.Log
import org.neo4j.procedure.Context
import org.neo4j.procedure.Description
import org.neo4j.procedure.Mode
import org.neo4j.procedure.Name
import org.neo4j.procedure.Procedure
import java.util.stream.Stream

class AzureProcedures {
    @Context
    @JvmField
    var log: Log? = null

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

        val convertedSource = convert(source)
        val batches = AzureClient.convertToBatch(convertedSource, nodeProperty)

        return AzureClient(config.getValue("url").toString(), config.getValue("key").toString(), log!!)
                .sentiment(batches, config["params"] as? Map<String, Any> ?: emptyMap())
                .map { AzureClient.responseToNodeWithMapResult(it, convertedSource) }
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

        val convertedSource = convert(source)
        val batches = AzureClient.convertToBatch(convertedSource, nodeProperty)

        return AzureClient(config.getValue("url").toString(), config.getValue("key").toString(), log!!)
                .entities(batches, config["params"] as? Map<String, Any> ?: emptyMap())
                .map { AzureClient.responseToNodeWithMapResult(it, convertedSource) }
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

        val convertedSource = convert(source)
        val batches = AzureClient.convertToBatch(convertedSource, nodeProperty)

        return AzureClient(config.getValue("url").toString(), config.getValue("key").toString(), log!!)
                .keyPhrases(batches, config["params"] as? Map<String, Any> ?: emptyMap())
                .map { AzureClient.responseToNodeWithMapResult(it, convertedSource) }
                .stream()
    }

//    @Procedure(value = "apoc.nlp.azure.vision.stream", mode = Mode.READ)
//    @Description("Provides a entity analysis for provided text")
//    fun vision(@Name("url") url: String,
//               @Name("key") key: String,
//               @Name("data") data: Any,
//               @Name(value = "config", defaultValue = "{}") config: Map<String, Any>)
//            : Stream<NodeWithMapResult> = AzureClient(url, key, log!!).vision(data, config).stream()

}