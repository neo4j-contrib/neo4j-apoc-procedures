package apoc.nlp.aws

import apoc.result.MapResult
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
    fun entities(@Name("apiKey") apiKey: String,
                 @Name("apiSecret") apiSecret: String,
                 @Name("data") data: Any,
                 @Name(value = "config", defaultValue = "{}") config: Map<String, Any>)
            : Stream<MapResult> = AWSClient(apiKey, apiSecret, log!!).entities(data, config).stream()
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