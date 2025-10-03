package apoc.ml;

import static apoc.ml.Prompt.API_KEY_CONF;

import apoc.util.Util;
import java.util.Map;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;

public class RagConfig {
    public static final String UNKNOWN_ANSWER = "Sorry, I don't know";
    public static final String DEFAULT_BASE_PROMPT = String.format(
            "You are a customer service agent that helps a customer with answering questions about a service.\n"
                    + "Use the following context to answer the `user question` at the end. Make sure not to make any changes to the context if possible when prepare answers to provide accurate responses.\n"
                    + "If you don't know the answer, just say `%s`, don't try to make up an answer.",
            UNKNOWN_ANSWER);

    public static final String EMBEDDINGS_CONF = "embeddings";
    public static final String GET_LABEL_TYPES_CONF = "getLabelTypes";
    public static final String TOP_K_CONF = "topK";
    public static final String PROMPT_CONF = "prompt";

    private final boolean getLabelTypes;
    private final EmbeddingQuery embeddings;
    private final Integer topK;
    private final String apiKey;
    private final String basePrompt;
    private final Map<String, Object> confMap;

    public RagConfig(Map<String, Object> confMap) {
        if (confMap == null) {
            confMap = Map.of();
        }

        this.confMap = confMap;
        this.getLabelTypes = Util.toBoolean(confMap.getOrDefault(GET_LABEL_TYPES_CONF, true));
        String embeddingString = (String) confMap.getOrDefault(EMBEDDINGS_CONF, EmbeddingQuery.Type.FALSE.name());
        this.embeddings = EmbeddingQuery.Type.valueOf(embeddingString).get();
        this.topK = Util.toInteger(confMap.getOrDefault(TOP_K_CONF, 40));
        this.apiKey = (String) confMap.get(API_KEY_CONF);
        this.basePrompt = (String) confMap.getOrDefault(PROMPT_CONF, DEFAULT_BASE_PROMPT);
    }

    public boolean isGetLabelTypes() {
        return getLabelTypes;
    }

    public EmbeddingQuery getEmbeddings() {
        return embeddings;
    }

    public Integer getTopK() {
        return topK;
    }

    public String getApiKey() {
        return apiKey;
    }

    public String getBasePrompt() {
        return basePrompt;
    }

    public Map<String, Object> getConfMap() {
        return confMap;
    }

    public interface EmbeddingQuery {
        Result getQuery(String queryOrIndex, String question, Transaction tx, RagConfig config);

        String BASE_EMBEDDING_QUERY = "CALL apoc.ml.openai.embedding([$question], $key , $conf)\n"
                + "YIELD index, text, embedding\n" + "WITH text, embedding";

        default Map<String, Object> getParams(String queryOrIndex, String question, RagConfig config) {
            return Map.of(
                    "vectorIndex",
                    queryOrIndex,
                    TOP_K_CONF,
                    config.getTopK(),
                    "question",
                    question,
                    "key",
                    config.getApiKey(),
                    "conf",
                    config.getConfMap());
        }

        enum Type {
            NODE(new Node()),
            REL(new Rel()),
            FALSE(new False());

            private final EmbeddingQuery embedding;

            Type(EmbeddingQuery embedding) {
                this.embedding = embedding;
            }

            public EmbeddingQuery get() {
                return embedding;
            }
        }

        class False implements EmbeddingQuery {
            @Override
            public Result getQuery(String queryOrIndex, String question, Transaction tx, RagConfig config) {
                return tx.execute(queryOrIndex);
            }
        }

        class Node implements EmbeddingQuery {
            @Override
            public Result getQuery(String queryOrIndex, String question, Transaction tx, RagConfig config) {
                return tx.execute(
                        BASE_EMBEDDING_QUERY
                                + "CALL db.index.vector.queryNodes($vectorIndex, $topK, embedding) YIELD node RETURN node",
                        getParams(queryOrIndex, question, config));
            }
        }

        class Rel implements EmbeddingQuery {
            @Override
            public Result getQuery(String queryOrIndex, String question, Transaction tx, RagConfig config) {
                return tx.execute(
                        BASE_EMBEDDING_QUERY
                                + "CALL db.index.vector.queryRelationships($vectorIndex, $topK, embedding) YIELD relationship RETURN relationship",
                        getParams(queryOrIndex, question, config));
            }
        }
    }
}
