package apoc.ml.bedrock;

import java.util.List;
import java.util.Map;

public class BedrockTestUtil {

    public static final Map<String, Object> STABILITY_AI_BODY = Map.of(
            "text_prompts", List.of(Map.of("text", "picture of a bird", "weight", 1.0)),
            "cfg_scale", 5,
            "seed", 123,
            "steps", 70,
            "style_preset", "photographic"
    );
    public static final Map<String, Object> JURASSIC_BODY = Map.of(
            "prompt", "Review: Extremely old cabinets, phone was half broken and full of dust. Bathroom door was broken, bathroom floor was dirty and yellow. Bathroom tiles were falling off. Asked to change my room and the next room was in the same conditions. The most out of date and least maintained hotel i ever been on. Extracted sentiment:",
            "maxTokens", 50,
            "temperature", 0,
            "topP", 1.0
    );
    public static final Map<String, Object> ANTHROPIC_CLAUDE_BODY = Map.of(
            "prompt", "\n\nHuman: Hello world\n\nAssistant:",
            "max_tokens_to_sample", 300,
            "temperature", 0.5,
            "top_k", 250,
            "top_p", 1,
            "stop_sequences", List.of("\\n\\nHuman:"),
            "anthropic_version", "bedrock-2023-05-31"
    );
    public static final Map<String, Object> TITAN_BODY = Map.of("inputText", "Test");

    public static final String BEDROCK_CUSTOM_PROC = "CALL apoc.ml.bedrock.custom($body, $conf)";

}
