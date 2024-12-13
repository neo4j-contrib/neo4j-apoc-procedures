package apoc.vectordb;

import static apoc.ml.RestAPIConfig.HEADERS_KEY;

import java.util.HashMap;
import java.util.Map;

public interface VectorDbHandler {
    default Map<String, Object> getCredentials(Object credentialsObj, Map<String, Object> config) {
        Map headers = (Map) config.getOrDefault(HEADERS_KEY, new HashMap<>());
        headers.putIfAbsent("Authorization", "Bearer " + credentialsObj);
        config.put(HEADERS_KEY, headers);
        return config;
    }

    String getUrl(String hostOrKey);

    VectorEmbeddingHandler getEmbedding();

    String getLabel();

    enum Type {
        CHROMA(new ChromaHandler()),
        QDRANT(new QdrantHandler()),
        PINECONE(new PineconeHandler()),
        WEAVIATE(new WeaviateHandler());

        private final VectorDbHandler handler;

        Type(VectorDbHandler handler) {
            this.handler = handler;
        }

        public VectorDbHandler get() {
            return handler;
        }
    }
}
