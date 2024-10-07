package apoc.ml;

import apoc.coll.Coll;
import apoc.meta.Meta;
import apoc.text.Strings;
import apoc.util.TestUtil;
import apoc.util.Util;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.List;
import java.util.Map;

import static apoc.ml.MLUtil.*;
import static apoc.ml.OpenAITestResultUtils.CHAT_COMPLETION_QUERY;
import static apoc.ml.OpenAITestResultUtils.COMPLETION_QUERY;
import static apoc.ml.OpenAITestResultUtils.EMBEDDING_QUERY;
import static apoc.ml.OpenAITestResultUtils.TEXT_TO_CYPHER_QUERY;
import static apoc.ml.OpenAITestResultUtils.assertChatCompletion;
import static apoc.ml.OpenAITestResultUtils.assertCompletion;
import static apoc.util.TestUtil.testCall;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;


/**
 * To start the tests, follow the instructions provided here: https://localai.io/basics/build/
 * Then, download the embedding model, as explained here: https://localai.io/models/#embeddings-bert 
 * Finally, set the env var `LOCAL_AI_URL=http://localhost:<portNumber>/v1`, default is `LOCAL_AI_URL=http://localhost:8080/v1`
 *
 * To test chatCompletionTomasonjo/text2CypherTomasonjo4Bit run localai with the command below:
 * ./local-ai run https://huggingface.co/tomasonjo/text2cypher-demo-4bit-gguf/resolve/main/text2cypher-demo-4bit-gguf-unsloth.Q4_K_M.gguf
 */
public class OpenAILocalAIIT {

    private String localAIUrl;
    private static final String OPENAI_KEY = System.getenv("OPENAI_KEY");

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();

    @Before
    public void setUp() throws Exception {
        localAIUrl = System.getenv("LOCAL_AI_URL");
        Assume.assumeNotNull("No LOCAL_AI_URL environment configured", localAIUrl);
        TestUtil.registerProcedure(db, OpenAI.class, Prompt.class, Meta.class, Strings.class, Coll.class);

        String movies = Util.readResourceFile("movies.cypher");
        try (Transaction tx = db.beginTx()) {
            tx.execute(movies);
            tx.commit();
        }

        String rag = Util.readResourceFile("rag.cypher");
        try (Transaction tx = db.beginTx()) {
            tx.execute(rag);
            tx.commit();
        }
    }

    @Test
    public void getEmbedding() {
        testCall(db, EMBEDDING_QUERY,
                getParams("text-embedding-ada-002"),
                row -> {
                    assertEquals(0L, row.get("index"));
                    assertEquals("Some Text", row.get("text"));
                    var embedding = (List<Double>) row.get("embedding");
                    assertEquals(384, embedding.size());
                });
    }

    @Test
    public void chatCompletionTomasonjo() {
        /*
        Useful terminal commands:
        # Run models
        ./local-ai run https://huggingface.co/tomasonjo/text2cypher-demo-4bit-gguf/resolve/main/text2cypher-demo-4bit-gguf-unsloth.Q4_K_M.gguf // List models
        ./local-ai run https://huggingface.co/tomasonjo/text2cypher-codestral-q4_k_m-gguf/resolve/main/text2cypher-codestral-q4_k_m-gguf-unsloth.Q4_K_M.gguf
        # List Models
        curl http://localhost:8080/v1/models
        # Call model
        curl http://localhost:8080/v1/chat/completions -H "Content-Type: application/json" -d '{"model":"text2cypher-demo-4bit-gguf-unsloth.Q4_K_M.gguf", "messages": [{"role": "user", "content": "What is the color of the sky? Answer in one word"}] }'
        */
        String[] models = {
                "text2cypher-demo-4bit-gguf-unsloth.Q4_K_M.gguf",
                "text2cypher-demo-8bit-gguf-unsloth.Q8_0.gguf",
        };

        for (String model : models) {
            testCall(db, CHAT_COMPLETION_QUERY,
                    getParams(model),
                    row -> assertChatCompletion(row, model));
        }
    }

    @Test
    public void text2CypherTomasonjo4Bit() {
        assertNotNull(OPENAI_KEY);
        String schema = """
                Node properties are the following:
                Movie {title: STRING, votes: INTEGER, tagline: STRING, released: INTEGER}, Person {born: INTEGER, name: STRING}
                Relationship properties are the following:
                ACTED_IN {roles: LIST}, REVIEWED {summary: STRING, rating: INTEGER}
                The relationships are the following:
                (:Person)-[:ACTED_IN]->(:Movie), (:Person)-[:DIRECTED]->(:Movie), (:Person)-[:PRODUCED]->(:Movie), (:Person)-[:WROTE]->(:Movie), (:Person)-[:FOLLOWS]->(:Person), (:Person)-[:REVIEWED]->(:Movie)
                """;

        String question = "Which actors played in the most movies?";
        String model = "text2cypher-demo-4bit-gguf-unsloth.Q4_K_M.gguf";

        Map<String, Object> params = Util.map(
                "schema", schema,
                "question", question
        );

        params.putAll(getParams(model));

        testCall(db, TEXT_TO_CYPHER_QUERY,
                params,
                row -> {
                    String cypherResult = assertChatCompletion(row, model);
                    // Check that is valid query
                    long count = TestUtil.count(db, cypherResult);
                    assertTrue(count > 0);
                });
    }

    @Test
    public void completion() {
        testCall(db, COMPLETION_QUERY,
                getParams("ggml-gpt4all-j"),
                (row) -> assertCompletion(row, "ggml-gpt4all-j"));
    }

    @Test
    public void chatCompletion() {
        testCall(db, CHAT_COMPLETION_QUERY, 
                getParams("ggml-gpt4all-j"),
                (row) -> assertChatCompletion(row, "ggml-gpt4all-j"));
    }

    private Map<String, Object> getParams(String model) {
        return Util.map("apiKey", "x",
                "conf", Map.of(ENDPOINT_CONF_KEY, localAIUrl,
                        MODEL_CONF_KEY, model)
        );
    }
}