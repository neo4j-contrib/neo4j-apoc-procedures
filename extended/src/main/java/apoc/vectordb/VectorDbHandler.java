package apoc.vectordb;

import apoc.util.UrlResolver;

import java.util.HashMap;
import java.util.Map;

import static apoc.ml.RestAPIConfig.HEADERS_KEY;
import static apoc.vectordb.VectorEmbeddingHandler.*;

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
        WEAVIATE(new WeaviateHandler());

        private final VectorDbHandler handler;

        Type(VectorDbHandler handler) {
            this.handler = handler;
        }

        public VectorDbHandler get() {
            return handler;
        }
    }

    //
    // -- implementations
    //

    class ChromaHandler implements VectorDbHandler {
        @Override
        public String getUrl(String hostOrKey) {
            return new UrlResolver("http", "localhost", 8000).getUrl("chroma", hostOrKey);
        }

        @Override
        public VectorEmbeddingHandler getEmbedding() {
            return new VectorEmbeddingHandler.ChromaEmbeddingHandler();
        }

        @Override
        public String getLabel() {
            return "Chroma";
        }
    }

    class QdrantHandler implements VectorDbHandler {
        @Override
        public String getUrl(String hostOrKey) {
            return new UrlResolver("http", "localhost", 6333).getUrl("qdrant", hostOrKey);
        }

        @Override
        public VectorEmbeddingHandler getEmbedding() {
            return new QdrantEmbeddingHandler();
        }

        @Override
        public String getLabel() {
            return "Qdrant";
        }
    }

    class WeaviateHandler implements VectorDbHandler {
        @Override
        public String getUrl(String hostOrKey) {
            String url = new UrlResolver("http", "localhost", 8000)
                    .getUrl("weaviate", hostOrKey);
            return url + "/v1";
        }

        @Override
        public VectorEmbeddingHandler getEmbedding() {
            return new VectorEmbeddingHandler.WeaviateEmbeddingHandler();
        }

        @Override
        public String getLabel() {
            return "Weaviate";
        }
    }
}
