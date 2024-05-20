package apoc.vectordb;


import apoc.ExtendedSystemPropertyKeys;
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
import static apoc.util.SystemDbUtil.withSystemDb;
import static apoc.vectordb.VectorEmbeddingConfig.MAPPING_KEY;
import static apoc.vectordb.VectorEmbeddingHandler.*;

public class VectorDbUtil {

    
    
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
