package apoc.vectordb;


import apoc.ExtendedSystemPropertyKeys;
import apoc.util.UrlResolver;
import apoc.util.Util;
import apoc.util.collection.Iterators;
import org.apache.commons.collections.MapUtils;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static apoc.ml.RestAPIConfig.BASE_URL_KEY;
import static apoc.ml.RestAPIConfig.ENDPOINT_KEY;
import static apoc.ml.RestAPIConfig.HEADERS_KEY;
import static apoc.util.SystemDbUtil.withSystemDb;
import static apoc.vectordb.VectorEmbeddingConfig.MAPPING_KEY;
import static apoc.vectordb.VectorEmbeddingHandler.*;

public class VectorDbUtil {
    
    interface VectorDbHandler {
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
            CHROMA(new VectorDbHandler.ChromaHandler()),
            QDRANT(new VectorDbHandler.QdrantHandler()),
            WEAVIATE(new VectorDbHandler.WeaviateHandler());

            private final VectorDbHandler handler;

            Type(VectorDbHandler handler) {
                this.handler = handler;
            }

            public VectorDbHandler get() {
                return handler;
            }
        }
        
        class ChromaHandler implements VectorDbHandler {
            @Override
            public String getUrl(String hostOrKey) {
                return new UrlResolver("http", "localhost", 8000).getUrl("chroma", hostOrKey);
            }

            @Override
            public VectorEmbeddingHandler getEmbedding() {
                return new ChromaEmbeddingHandler();
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
                return new WeaviateEmbeddingHandler();
            }

            @Override
            public String getLabel() {
                return "Weaviate";
            }
        }
    }
    
    
    /**
     * we can configure the endpoint via config map or via hostOrKey parameter,
     * to handle potential endpoint changes.
     * For example, in Qdrant `BASE_URL/collections/COLLECTION_NAME/points` could change in the future.
     */
    public static void getEndpoint(Map<String, Object> config, String endpoint) {
        config.putIfAbsent(ENDPOINT_KEY, endpoint);
    }

    /**
     * Result of `apoc.vectordb.*.get` and `apoc.vectordb.*.query` procedures
     */
    public record EmbeddingResult(
            Object id, Double score, List<Double> vector, Map<String, Object> metadata, String text,
            Node node,
            Relationship rel) {}

    
    public static Map<String, Object> getCommonVectorDbInfo(
            String hostOrKey, String collection, Map<String, Object> configuration, String templateUrl, VectorDbHandler handler) {
        Map<String, Object> config = new HashMap<>(configuration);

        Map<String, Object> props = withSystemDb(transaction -> {
            Label label = Label.label(handler.getLabel());
            Node node = Iterators.singleOrNull(transaction.findNodes(label));
            return node == null ? Map.of() : node.getAllProperties();
        });

        String url = hostOrKey == null
                ? (String) props.get(ExtendedSystemPropertyKeys.host.name())
                : handler.getUrl(hostOrKey);
        config.put(BASE_URL_KEY, url);

        Map mappingConfVal = (Map) config.get(MAPPING_KEY);
        if ( MapUtils.isEmpty(mappingConfVal) ) {
            String mappingStoreVal = (String) props.get(MAPPING_KEY);
            if (mappingStoreVal != null) {
                config.put( MAPPING_KEY, Util.fromJson(mappingStoreVal, Map.class) );
            }
        }

        String credentials = (String) props.get(ExtendedSystemPropertyKeys.credentials.name());
        if (credentials != null) {
            Object credentialsObj = Util.fromJson(credentials, Object.class);
            
            config = handler.getCredentials(credentialsObj, config);
        }

        String endpoint = templateUrl.formatted(url, collection);
        getEndpoint(config, endpoint);

        return config;
    }

}
