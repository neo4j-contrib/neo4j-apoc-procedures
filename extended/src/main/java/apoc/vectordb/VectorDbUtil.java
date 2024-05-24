package apoc.vectordb;


import apoc.ExtendedSystemPropertyKeys;
import apoc.SystemPropertyKeys;
import apoc.result.MapResult;
import apoc.util.Util;
import org.apache.commons.collections.MapUtils;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static apoc.ml.RestAPIConfig.BASE_URL_KEY;
import static apoc.ml.RestAPIConfig.ENDPOINT_KEY;
import static apoc.util.SystemDbUtil.withSystemDb;
import static apoc.vectordb.VectorEmbeddingConfig.MAPPING_KEY;

public class VectorDbUtil {

    public static final String ERROR_READONLY_MAPPING = "The mapping is not possible with this procedure, as it is read-only.";

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
            Node node = transaction.findNode(label, SystemPropertyKeys.name.name(), hostOrKey);
            return node == null ? Map.of() : node.getAllProperties();
        });

        String url = getUrl(hostOrKey, handler, props);
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
    
    public static Stream<MapResult> getInfoProcCommon(String hostOrKey, VectorDbHandler handler) {
        Map<String, Object> info = getCommonVectorDbInfo(hostOrKey, "", Map.of(), "%s", handler);
        // endpoint is equivalent to baseUrl config
        info.remove("endpoint");
        return Stream.of(new MapResult(info));
    }

    private static String getUrl(String hostOrKey, VectorDbHandler handler, Map<String, Object> props) {
        if (props.isEmpty()) {
            return handler.getUrl(hostOrKey);
        }
        return (String) props.get(ExtendedSystemPropertyKeys.host.name());
    }

    public static void checkMappingConf(Map<String, Object> configuration, String procName) {
        if (configuration.containsKey(MAPPING_KEY)) {
            throw new RuntimeException(ERROR_READONLY_MAPPING + "\n" +
                                       "Try the equivalent procedure, which is the " + procName);
        }
    }

}
