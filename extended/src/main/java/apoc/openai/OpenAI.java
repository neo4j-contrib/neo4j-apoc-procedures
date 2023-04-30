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
  @Description("apoc.openai.getEmbedding(text, api_key, model) - returns the embeddings for a given text")
  public Stream < MapResult > getEmbedding(@Name("text") String text, @Name("api_key") String apiKey, @Name(value = "model", defaultValue = "text-embedding-ada-002") String model) throws Exception {
    String endpoint = "https://api.openai.com/v1/embeddings";
    Map < String, Object > headers = new HashMap < > ();
    headers.put("Content-Type", "application/json");
    headers.put("Authorization", "Bearer " + apiKey);
    String payload = "{\n" +
      "  \"model\": \"" + model + "\",\n" +
      "  \"input\": \"" + text + "\"\n" +
      "}";
    Stream < Object > value = JsonUtil.loadJson(endpoint, headers, payload, "", true, new ArrayList < > ());
    Map < String, Object > map = value.collect(Collectors.toMap(k -> "embedding", v -> ((Map < String, List < Map < String, Object >>> ) v).get("data").get(0).get("embedding")));
    return Stream.of(new MapResult(map));
  }

  @Procedure
  @Description("apoc.openai.completion(text, api_key, configuration) - prompts the completion API")
  public Stream < MapResult > completion(@Name("prompt") String prompt, @Name("api_key") String apiKey, @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) throws Exception {
    String endpoint = "https://api.openai.com/v1/completions";
    Map < String, Object > headers = new HashMap < > ();
    headers.put("Content-Type", "application/json");
    headers.put("Authorization", "Bearer " + apiKey);

    configuration.put("prompt", prompt);
    if (!configuration.containsKey("model")) {
        configuration.put("model", "text-davinci-003");
    }

    String payload = new ObjectMapper().writeValueAsString(configuration);

    Stream < Object > value = JsonUtil.loadJson(endpoint, headers, payload, "", true, new ArrayList < > ());
    Map < String, Object > map = value.collect(Collectors.toMap(k -> "results", v -> ((Map < String, List < Map < String, Object >>> ) v).get("choices").get(0).get("text")));
    return Stream.of(new MapResult(map));
  }
}