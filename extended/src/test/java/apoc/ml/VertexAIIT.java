package apoc.ml;

import apoc.util.TestUtil;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.List;
import java.util.Map;

import static apoc.util.TestUtil.testCall;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class VertexAIIT {

    private String vertexAiKey;
    private String vertexAiProject;

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();
    private Map<String, Object> parameters;

    public VertexAIIT() {
    }

    @Before
    public void setUp() throws Exception {
        vertexAiKey = System.getenv("VERTEXAI_KEY");
        Assume.assumeNotNull("No VERTEXAI_KEY environment configured", vertexAiKey);
        vertexAiProject = System.getenv("VERTEXAI_PROJECT");
        Assume.assumeNotNull("No VERTEXAI_PROJECT environment configured", vertexAiProject);
        TestUtil.registerProcedure(db, VertexAI.class);
        parameters = Map.of("apiKey", vertexAiKey, "project", vertexAiProject);
    }

    @Test
    public void getEmbedding() {
        testCall(db, "CALL apoc.ml.vertexai.embedding(['Some Text'], $apiKey, $project)", parameters,(row) -> {
            System.out.println("row = " + row);
            assertEquals(0L, row.get("index"));
            assertEquals("Some Text", row.get("text"));
            var embedding = (List<Double>) row.get("embedding");
            assertEquals(768, embedding.size());
            assertEquals(true, embedding.stream().allMatch(d -> d instanceof Double));
        });
    }

    @Test
    public void completion() {
        testCall(db, "CALL apoc.ml.vertexai.completion('What color is the sky? Answer in one word: ', $apiKey, $project)", parameters,(row) -> {
                    System.out.println("row = " + row);
            var result = (Map<String,Object>)row.get("value");
            // {value={safetyAttributes={blocked=false, scores=[0.1], categories=[Sexual]}, recitationResult={recitations=[], recitationAction=NO_ACTION}, content=blue}}
            assertEquals(true, result.containsKey("safetyAttributes"));
            var safetyAttributes = (Map) result.get("safetyAttributes");
            assertEquals(false, safetyAttributes.get("blocked"));
            assertEquals(true, safetyAttributes.containsKey("categories"));

            String text = (String) result.get("content");
            assertEquals(true, text != null && !text.isBlank());
            assertEquals(true, text.toLowerCase().contains("blue"));

            assertEquals(true, result.containsKey("recitationResult"));
            assertEquals("NO_ACTION", ((Map)result.get("recitationResult")).get("recitationAction"));
        });
    }

    @Test
    public void chatCompletion() {
        testCall(db, """
CALL apoc.ml.vertexai.chat([
{author:"user", content:"What planet do timelords live on?"}
],  $apiKey, $project, {temperature:0},
"Fictional universe of Doctor Who. Only answer with a single word!",
[{input:{content:"What planet do humans live on?"}, output:{content:"Earth"}}])
""", parameters, (row) -> {
            System.out.println("row = " + row);
            // {value={candidates=[{author=1, content=Gallifrey.}], safetyAttributes={blocked=false, scores=[0.1, 0.1, 0.1], categories=[Religion & Belief, Sexual, Toxic]}, recitationResults=[{recitations=[], recitationAction=NO_ACTION}]}}
            var result = (Map<String,Object>)row.get("value");

            assertEquals(true, result.containsKey("safetyAttributes"));
            var safetyAttributes = (Map) result.get("safetyAttributes");
            assertEquals(false, safetyAttributes.get("blocked"));
            assertEquals(true, safetyAttributes.containsKey("categories"));
            assertEquals(3, ((List)safetyAttributes.get("categories")).size());

            assertEquals(true, result.containsKey("recitationResults"));
            assertEquals("NO_ACTION", ((List<Map>)result.get("recitationResults")).get(0).get("recitationAction"));

            var candidates = (List<Map<String,Object>>)result.get("candidates");
            var author = candidates.get(0).get("author");
            assertEquals("1", author);

            var text = (String)candidates.get(0).get("content");
            assertEquals(true, text != null && !text.isBlank());
            assertEquals(true, text.toLowerCase().contains("gallifrey"));
        });
    }
}