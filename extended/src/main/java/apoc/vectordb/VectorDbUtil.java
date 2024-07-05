package apoc.vectordb;


import apoc.ExtendedSystemPropertyKeys;
import apoc.SystemPropertyKeys;
import apoc.util.ExtendedMapUtils;
import apoc.util.Util;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static apoc.ml.RestAPIConfig.BASE_URL_KEY;
import static apoc.ml.RestAPIConfig.ENDPOINT_KEY;
import static apoc.util.SystemDbUtil.withSystemDb;
import static apoc.vectordb.VectorEmbeddingConfig.FIELDS_KEY;
import static apoc.vectordb.VectorEmbeddingConfig.MAPPING_KEY;
import static apoc.vectordb.VectorEmbeddingConfig.METADATA_KEY;
import static apoc.vectordb.VectorMappingConfig.MODE_KEY;
import static apoc.vectordb.VectorMappingConfig.MappingMode.READ_ONLY;

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
            Relationship rel,
            Object errors) {}
    
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
        if ( ExtendedMapUtils.isEmpty(mappingConfVal) ) {
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

    private static String getUrl(String hostOrKey, VectorDbHandler handler, Map<String, Object> props) {
        if (props.isEmpty()) {
            return handler.getUrl(hostOrKey);
        }
        return (String) props.get(ExtendedSystemPropertyKeys.host.name());
    }

    public static void setReadOnlyMappingMode(Map<String, Object> configuration) {
        Map<String, Object> mappingConf = (Map<String, Object>) configuration.getOrDefault(MAPPING_KEY, new HashMap<>());
        mappingConf.put(MODE_KEY, READ_ONLY.toString());
    }

    /**
     * get field list, with the API requiring a list of field keys to be returned,
     * e.g. the weaviate and the milvus query APIs
     * 
     * It returns the fields defined via `fields` config plus the one present in the `metadataKey`, within the config `mapping`
     */
    public static List getFieldList(Map<String, Object> config) {
        Set fields = new HashSet<>();
        
        List fieldListConfig = (List) config.get(FIELDS_KEY);
        if (fieldListConfig != null) {
            fields.addAll(fieldListConfig);
        }
        
        Map<String, String> mappingKey = (Map) config.getOrDefault(MAPPING_KEY, Map.of());
        String metadataKey = mappingKey.get(METADATA_KEY);
        if (metadataKey != null) {
            fields.add(metadataKey);
        }
        
        if (fields.isEmpty()) {
            throw new RuntimeException("You have to define `field` list of parameter with the fields to be returned" +
                                       "or a `mapping` config with the `medadataKey` as the field to be returned");
        }
        return new ArrayList<>(fields);
    }
}
