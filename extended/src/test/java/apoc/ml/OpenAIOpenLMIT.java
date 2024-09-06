package apoc.ml;

import apoc.util.TestUtil;
import apoc.util.Util;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static apoc.ml.OpenAI.API_TYPE_CONF_KEY;
import static apoc.ml.MLUtil.*;
import static apoc.ml.OpenAI.PATH_CONF_KEY;
import static apoc.ml.OpenAITestResultUtils.*;
import static apoc.util.TestUtil.testCall;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * OpenLM-like tests for Cohere and HuggingFace, see here: https://github.com/r2d4/openlm
 * 
 * NB: It works only for `Completion` API, as described in the README.md:
 * https://github.com/r2d4/openlm/blob/main/README.md?plain=1#L36
 */
public class OpenAIOpenLMIT {
    
    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();


    @Before
    public void setUp() throws Exception {
        TestUtil.registerProcedure(db, OpenAI.class);
    }

    /**
     * Request converter similar to: https://github.com/r2d4/openlm/blob/main/openlm/llm/huggingface.py
     */
    @Test
    public void completionWithHuggingFace() {
        String huggingFaceApiKey = System.getenv("HF_API_TOKEN");
        Assume.assumeNotNull("No HF_API_TOKEN environment configured", huggingFaceApiKey);

        String modelId = "google-bert/bert-base-uncased";
        Map<String, String> conf = Map.of(ENDPOINT_CONF_KEY, "https://api-inference.huggingface.co/models/" + modelId,
                API_TYPE_CONF_KEY, OpenAIRequestHandler.Type.HUGGINGFACE.name()
        );

        testCall(db, "CALL apoc.ml.openai.completion('The sky has a [MASK] color', $apiKey, $conf)",
                Map.of("conf", conf, "apiKey", huggingFaceApiKey),
                (row) -> {
                    var result = (Map<String,Object>) row.get("value");
                    String generatedText = (String) result.get("sequence");
                    assertTrue(generatedText.toLowerCase().contains("blue"),
                            "Actual generatedText is " + generatedText);
                });
    }

    /**
     * Request converter similar to: https://github.com/r2d4/openlm/blob/main/openlm/llm/cohere.py
     */
    @Test
    public void completionWithCohere() {
        String cohereApiKey = System.getenv("COHERE_API_TOKEN");
        Assume.assumeNotNull("No COHERE_API_TOKEN environment configured", cohereApiKey);
        
        String modelId = "command";
        Map<String, String> conf = Map.of(ENDPOINT_CONF_KEY, "https://api.cohere.ai/v1/generate",
                PATH_CONF_KEY, "",
                MODEL_CONF_KEY, modelId
        );
        
        testCall(db, COMPLETION_QUERY,
                Map.of("conf", conf, "apiKey", cohereApiKey),
                (row) -> {
                    var result = (Map<String,Object>) row.get("value");
                    Map meta = (Map) result.get("meta");
                    assertEquals(Set.of("warnings", "billed_units", "api_version"), meta.keySet());

                    List<Map> generations = (List<Map>) result.get("generations");
                    assertEquals(1, generations.size());
                    assertEquals(Set.of("finish_reason", "id", "text"), generations.get(0).keySet());
                    
                    assertTrue(result.get("id") instanceof String);
                    assertTrue(result.get("prompt") instanceof String);
                });
    }
}