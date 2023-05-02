package apoc.openai;

import apoc.Extended;
import apoc.util.JsonUtil;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.List;
import java.util.ArrayList;
import java.util.stream.Stream;
import java.util.stream.Collectors;
import apoc.result.MapResult;

import com.fasterxml.jackson.databind.ObjectMapper;



@Extended
public class OpenAI {

  @Procedure
  @Description("apoc.openai.getEmbedding([texts], api_key, model) - returns the embeddings for a given text")
  public Stream < MapResult > getEmbedding(@Name("texts") List<String> texts, @Name("api_key") String apiKey, @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) throws Exception {
    String endpoint = "https://api.openai.com/v1/embeddings";
    Map<String, Object> headers = Map.of(
      "Content-Type", "application/json",
      "Authorization", "Bearer " + apiKey
  );

    if (!configuration.containsKey("model")) {
      configuration.put("model", "text-embedding-ada-002");
    }
    configuration.put("input", texts);

    String payload = new ObjectMapper().writeValueAsString(configuration);
    Stream < MapResult > value = JsonUtil.loadJson(endpoint, headers, payload, "", true, new ArrayList < > ()).map(v -> Map.of("embedding", v)).map(MapResult::new);
    return value;
  }

  @Procedure
  @Description("apoc.openai.completion(prompt, api_key, configuration) - prompts the completion API")
  public Stream < MapResult > completion(@Name("prompt") String prompt, @Name("api_key") String apiKey, @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) throws Exception {
    String endpoint = "https://api.openai.com/v1/completions";
    Map<String, Object> headers = Map.of(
      "Content-Type", "application/json",
      "Authorization", "Bearer " + apiKey
  );

    configuration.put("prompt", prompt);
    if (!configuration.containsKey("model")) {
        configuration.put("model", "text-davinci-003");
    }

    String payload = new ObjectMapper().writeValueAsString(configuration);

    Stream < MapResult > value = JsonUtil.loadJson(endpoint, headers, payload, "", true, new ArrayList < > ()).map(v -> Map.of("results", v)).map(MapResult::new);
    return value;
  }

  @Procedure
  @Description("apoc.openai.chatCompletion(messages, api_key, configuration) - prompts the completion API")
  public Stream < MapResult > chatCompletion(@Name("messages") List<Map<String, Object>> messages, @Name("api_key") String apiKey, @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) throws Exception {
    String endpoint = "https://api.openai.com/v1/chat/completions";
    Map<String, Object> headers = Map.of(
      "Content-Type", "application/json",
      "Authorization", "Bearer " + apiKey
  );

    configuration.put("messages", messages);
    if (!configuration.containsKey("model")) {
        configuration.put("model", "gpt-3.5-turbo");
    }

    String payload = new ObjectMapper().writeValueAsString(configuration);

    Stream < MapResult > value = JsonUtil.loadJson(endpoint, headers, payload, "", true, new ArrayList < > ()).map(v -> Map.of("results", v)).map(MapResult::new);
    return value;
  }
}