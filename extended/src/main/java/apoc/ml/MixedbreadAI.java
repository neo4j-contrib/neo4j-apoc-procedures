package apoc.ml;

import apoc.ApocConfig;
import apoc.result.ObjectResult;
import org.neo4j.graphdb.security.URLAccessChecker;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static apoc.ml.OpenAI.API_TYPE_CONF_KEY;
import static apoc.ml.OpenAI.APOC_ML_OPENAI_URL;
import static apoc.ml.OpenAI.MODEL_CONF_KEY;

public class MixedbreadAI {
    
    public static final String ENDPOINT_CONF_KEY = "endpoint";
    public static final String DEFAULT_MODEL_ID = "mxbai-embed-large-v1";
    public static final String MIXEDBREAD_BASE_URL = "https://api.mixedbread.ai/v1";
    public static final String ERROR_MSG_MISSING_ENDPOINT = "The endpoint must be defined via config `%s` or via apoc.conf `%s`"
            .formatted(ENDPOINT_CONF_KEY, APOC_ML_OPENAI_URL);
    
    public static final String ERROR_MSG_MISSING_MODELID = "The model must be defined via config `%s`"
            .formatted(MODEL_CONF_KEY);


    /**
         * embedding is an Object instead of List<Double>, as with a Mixedbread request having `"encoding_format": [<multipleFormat>]`,
         * the result can be e.g. {... "embedding": { "float": [<floatEmbedding>], "base": <base64Embedding>,   } ...}
         * instead of e.g. {... "embedding": [<floatEmbedding>] ...}
         */
    public record EmbeddingResult(long index, String text, Object embedding) { }
    
    @Context
    public URLAccessChecker urlAccessChecker;
    
    @Context
    public ApocConfig apocConfig;


    @Procedure("apoc.ml.mixedbread.custom")
    @Description("apoc.mixedbread.custom(, configuration) - returns the embeddings for a given text")
    public Stream<ObjectResult> custom(@Name("api_key") String apiKey, @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) throws Exception {
        if (!configuration.containsKey(MODEL_CONF_KEY)) {
            throw new RuntimeException(ERROR_MSG_MISSING_MODELID);
        }
        
        configuration.put(API_TYPE_CONF_KEY, OpenAIRequestHandler.Type.MIXEDBREAD_CUSTOM.name());
        
        return OpenAI.executeRequest(apiKey, configuration, 
                        null, null, null, null, null, 
                        apocConfig, 
                        urlAccessChecker)
                .map(ObjectResult::new);
    }
    

    @Procedure("apoc.ml.mixedbread.embedding")
    @Description("apoc.mixedbread.mixedbread([texts], api_key, configuration) - returns the embeddings for a given text")
    public Stream<EmbeddingResult> getEmbedding(@Name("texts") List<String> texts,
                                                       @Name("api_key") String apiKey,
                                                       @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) throws Exception {
        configuration.putIfAbsent(MODEL_CONF_KEY, DEFAULT_MODEL_ID);

        configuration.put(API_TYPE_CONF_KEY, OpenAIRequestHandler.Type.MIXEDBREAD_EMBEDDING.name());
        return OpenAI.getEmbeddingResult(texts, apiKey, configuration, apocConfig, urlAccessChecker,
                (map, text) -> {
                    Long index = (Long) map.get("index");
                    return new EmbeddingResult(index, text, map.get("embedding"));
                },
                m -> new EmbeddingResult(-1, m, List.of())
        );

    }

}
