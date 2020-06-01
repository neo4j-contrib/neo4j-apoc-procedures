package apoc.ai.azure

import org.neo4j.logging.Log
import org.neo4j.procedure.*
import java.util.stream.Stream

class AzureProcedures {
    @Context
    @JvmField
    var log: Log? = null

//    @Procedure(value = "apoc.ai.azure.sentiment", mode = Mode.READ)
//    @Description("Provides a sentiment analysis for provided text")
//    fun sentiment(@Name("url") url: String,
//                  @Name("key") key: String,
//                  @Name("data") data: Any,
//                  @Name(value = "config", defaultValue = "{}") config: Map<String, Any>)
//            : Stream<AIMapResult> = AzureClient(url, key, log!!).sentiment(data, config).stream()
//
//    @Procedure(value = "apoc.ai.azure.entities", mode = Mode.READ)
//    @Description("Provides a entity analysis for provided text")
//    fun entities(@Name("url") url: String,
//                 @Name("key") key: String,
//                 @Name("data") data: Any,
//                 @Name(value = "config", defaultValue = "{}") config: Map<String, Any>)
//            : Stream<AIMapResult> = AzureClient(url, key, log!!).entities(data, config).stream()
//
//    @Procedure(value = "apoc.ai.azure.keyPhrases", mode = Mode.READ)
//    @Description("Provides a entity analysis for provided text")
//    fun keyPhrases(@Name("url") url: String,
//                   @Name("key") key: String,
//                   @Name("data") data: Any,
//                   @Name(value = "config", defaultValue = "{}") config: Map<String, Any>)
//            : Stream<AIMapResult> = AzureClient(url, key, log!!).keyPhrases(data, config).stream()
//
//    @Procedure(value = "apoc.ai.azure.vision", mode = Mode.READ)
//    @Description("Provides a entity analysis for provided text")
//    fun vision(@Name("url") url: String,
//               @Name("key") key: String,
//               @Name("data") data: Any,
//               @Name(value = "config", defaultValue = "{}") config: Map<String, Any>)
//            : Stream<AIMapResult> = AzureClient(url, key, log!!).vision(data, config).stream()

}