package apoc.openai;

import apoc.Extended;
import apoc.util.JsonUtil;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.stream.Stream;
import java.util.stream.Collectors;
import apoc.result.MapResult;

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
}