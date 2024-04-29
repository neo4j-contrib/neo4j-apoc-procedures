package apoc.ml;

import apoc.util.TestUtil;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static apoc.ml.MixedbreadAI.*;
import static apoc.ml.OpenAI.MODEL_CONF_KEY;
import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testResult;
import static apoc.util.Util.map;
import static java.util.Collections.emptyMap;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

public class MixedbreadAIIT {

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    private static String apiKey;

    @BeforeClass
    public static void setUp() throws Exception {
        String keyIdEnv = "MIXEDBREAD_API_KEY";
        apiKey = System.getenv(keyIdEnv);

        Assume.assumeNotNull("No MIXEDBREAD_API_KEY environment configured", apiKey);

        TestUtil.registerProcedure(db, MixedbreadAI.class);
    }

    @Test
    public void getEmbedding() {
        testResult(db, "CALL apoc.ml.mixedbread.embedding(['Some Text', 'Other Text'], $apiKey, $conf)", 
                map("apiKey", apiKey, "conf", emptyMap()),
                r -> {
                    Map<String, Object> row = r.next();
                    assertEmbedding(row, 0L, "Some Text", 1024);

                    row = r.next();
                    assertEmbedding(row, 1L, "Other Text", 1024);
                    
                    assertFalse(r.hasNext());
                });
    }

    @Test
    public void getEmbeddingWithNulls() {
        testResult(db, "CALL apoc.ml.mixedbread.embedding([null, 'Some Text', null, 'Another Text'], $apiKey, $conf)", 
                Map.of("apiKey", apiKey, "conf", emptyMap()),
                (r) -> {

                    Map<String, Object> row = r.next();
                    assertEquals(1024, ((List) row.get("embedding")).size());
                    assertEquals("Some Text", row.get("text"));

                    row = r.next();
                    assertEquals(1024, ((List) row.get("embedding")).size());
                    assertEquals("Another Text", row.get("text"));

                    row = r.next();
                    assertNullEmbedding(row);
                    
                    row = r.next();
                    assertNullEmbedding(row);

                    assertFalse(r.hasNext());
                });
    }
    
    @Test
    public void getEmbeddingWithCustomMultipleEncodingFormats() {
        Set<String> formats = Set.of("float", "binary", "ubinary", "int8", "uint8", "base64");
        Map<String, Object> conf = map("encoding_format", formats);
        testResult(db, "CALL apoc.ml.mixedbread.embedding(['Some Text', 'Other Text'], $apiKey, $conf)", 
                map("apiKey", apiKey, "conf", conf),
                r -> {
                    Map<String, Object> row = r.next();
                    assertEquals(0L, row.get("index"));
                    assertEquals("Some Text", row.get("text"));
                    var embedding = (Map) row.get("embedding");
                    assertEquals(formats, embedding.keySet());
                    assertTrue(embedding.get("float") instanceof List);
                    assertTrue(embedding.get("binary") instanceof List);
                    assertTrue(embedding.get("ubinary") instanceof List);
                    assertTrue(embedding.get("int8") instanceof List);
                    assertTrue(embedding.get("uint8") instanceof List);
                    assertTrue(embedding.get("base64") instanceof String);

                    row = r.next();
                    assertEquals(1L, row.get("index"));
                    assertEquals("Other Text", row.get("text"));
                    embedding = (Map) row.get("embedding");
                    assertEquals(formats, embedding.keySet());
                    assertTrue(embedding.get("float") instanceof List);
                    assertTrue(embedding.get("binary") instanceof List);
                    assertTrue(embedding.get("ubinary") instanceof List);
                    assertTrue(embedding.get("int8") instanceof List);
                    assertTrue(embedding.get("uint8") instanceof List);
                    assertTrue(embedding.get("base64") instanceof String);

                    assertFalse(r.hasNext());
                });
    }

    @Test
    public void getEmbeddingWithCustomEmbeddingSize() {
        testResult(db, "CALL apoc.ml.mixedbread.embedding(['Some Text', 'Other Text'], $apiKey, $conf)",
                map("apiKey", apiKey, "conf", map("dimensions", 256)),
                r -> {
                    Map<String, Object> row = r.next();
                    assertEmbedding(row, 0L, "Some Text", 256);

                    row = r.next();
                    assertEmbedding(row, 1L, "Other Text", 256);

                    assertFalse(r.hasNext());
                });
    }

    @Test
    public void getEmbeddingWithOtherModel() {
        testResult(db, "CALL apoc.ml.mixedbread.embedding(['Some Text', 'Other Text'], $apiKey, $conf)",
                map("apiKey", apiKey, "conf", map(MODEL_CONF_KEY, "mxbai-embed-2d-large-v1")),
                r -> {
                    Map<String, Object> row = r.next();
                    assertEmbedding(row, 0L, "Some Text", 1024);

                    row = r.next();
                    assertEmbedding(row, 1L, "Other Text", 1024);

                    assertFalse(r.hasNext());
                });
    }

    @Test
    public void getEmbeddingWithWrongModel() {
        try {
            testCall(db, "CALL apoc.ml.mixedbread.embedding(['Some Text', 'Other Text'], $apiKey, $conf)",
                    map("apiKey", apiKey, 
                            "conf", map(MODEL_CONF_KEY, "wrong-id")
                    ),
                    r -> fail("Should fail due to wrong model id"));
        } catch (Exception e) {
            String errMsg = e.getMessage();
            assertTrue("Actual error message is: " + errMsg,
                    errMsg.contains("Server returned HTTP response code: 422 for URL: https://api.mixedbread.ai/v1/embeddings")
            );
            
        }
    }

    /**
     * Example taken from here: https://www.mixedbread.ai/api-reference/endpoints/reranking 
     */
    @Test
    public void customWithReranking() {
        List<String> input = List.of("To Kill a Mockingbird is a novel by Harper Lee published in 1960. It was immediately successful, winning the Pulitzer Prize, and has become a classic of modern American literature.",
                "The novel Moby-Dick was written by Herman Melville and first published in 1851. It is considered a masterpiece of American literature and deals with complex themes of obsession, revenge, and the conflict between good and evil.",
                "Harper Lee, an American novelist widely known for her novel To Kill a Mockingbird, was born in 1926 in Monroeville, Alabama. She received the Pulitzer Prize for Fiction in 1961.",
                "Jane Austen was an English novelist known primarily for her six major novels, which interpret, critique and comment upon the British landed gentry at the end of the 18th century.",
                "The Harry Potter series, which consists of seven fantasy novels written by British author J.K. Rowling, is among the most popular and critically acclaimed books of the modern era.",
                "The Great Gatsby, a novel written by American author F. Scott Fitzgerald, was published in 1925. The story is set in the Jazz Age and follows the life of millionaire Jay Gatsby and his pursuit of Daisy Buchanan."
        );
        Map<String, Object> conf = map(ENDPOINT_CONF_KEY, MIXEDBREAD_BASE_URL + "/reranking",
                MODEL_CONF_KEY, "mixedbread-ai/mxbai-rerank-large-v1", 
                "query", "Who is the author of To Kill a Mockingbird?",
                "top_k", 3,
                "input", input
        );
        testCall(db, "CALL apoc.ml.mixedbread.custom($apiKey, $conf)",
                Map.of("apiKey", apiKey, "conf", conf),
                row -> {
                    Map value = (Map) row.get("value");
                    
                    List<Map> data = (List<Map>) value.get("data");
                    assertEquals(3, data.size());
                    
                    Map<String, Object> firstData = map("index", 0L, 
                            "score", 0.9980469, 
                            "object", "text_document");
                    assertEquals(firstData, data.get(0));


                    Map<String, Object> secondData = map(
                            "index", 2L,
                            "score", 0.9980469,
                            "object", "text_document");
                    assertEquals(secondData, data.get(1));

                    Map<String, Object> thirdData = map(
                            "index", 3L,
                            "score", 0.06915283,
                            "object", "text_document");
                    assertEquals(thirdData, data.get(2));
                    
                    assertEquals("list", value.get("object"));
                });
    }

    @Test
    public void customWithMissingEndpoint() {
        try {
            testCall(db, "CALL apoc.ml.mixedbread.custom($apiKey, $conf)",
                    map("apiKey", apiKey,
                            "conf", map(MODEL_CONF_KEY, "aModelId")
                    ),
                    r -> fail("Should fail due to missing endpoint"));
        } catch (Exception e) {
            String errMsg = e.getMessage();
            assertTrue("Actual error message is: " + errMsg, 
                    errMsg.contains(ERROR_MSG_MISSING_ENDPOINT)
            );
        }
    }

    @Test
    public void customWithMissingModel() {
        try {
            testCall(db, "CALL apoc.ml.mixedbread.custom($apiKey, $conf)",
                    map("apiKey", apiKey,
                            "conf", map(ENDPOINT_CONF_KEY, MIXEDBREAD_BASE_URL + "/reranking",
                                    "foo", "bar")
                    ),
                    r -> fail("Should fail due to missing model"));
        } catch (Exception e) {
            String errMsg = e.getMessage();
            assertTrue("Actual error message is: " + errMsg,
                    errMsg.contains(ERROR_MSG_MISSING_MODELID)
            );
        }
    }

    private static void assertEmbedding(Map<String, Object> row,
                                        long expectedIdx,
                                        String expectedText,
                                        Integer expectedSize) {
        assertEquals(expectedIdx, row.get("index"));
        assertEquals(expectedText, row.get("text"));
        var embedding = (List<Double>) row.get("embedding");
        assertEquals(expectedSize, embedding.size());
    }

    private static void assertNullEmbedding(Map<String, Object> row) {
        assertEmbedding(row, -1, null, 0);
    }
                
}
