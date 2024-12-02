package apoc.vectordb;

import static apoc.ml.RestAPIConfig.BASE_URL_KEY;
import static apoc.ml.RestAPIConfig.ENDPOINT_KEY;
import static apoc.util.SystemDbUtil.withSystemDb;
import static apoc.vectordb.VectorEmbeddingConfig.MAPPING_KEY;

import apoc.SystemPropertyKeys;
import apoc.util.Util;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections.MapUtils;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

public class VectorDbUtil {

    public static final String ERROR_READONLY_MAPPING =
            "The mapping is not possible with this procedure, as it is read-only.";

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
    public static final class EmbeddingResult {
        public final Object id;
        public final Double score;
        public final List<Double> vector;
        public final Map<String, Object> metadata;
        public final String text;
        public final Node node;
        public final Relationship rel;

        public EmbeddingResult(
                Object id,
                Double score,
                List<Double> vector,
                Map<String, Object> metadata,
                String text,
                Node node,
                Relationship rel) {
            this.id = id;
            this.score = score;
            this.vector = vector;
            this.metadata = metadata;
            this.text = text;
            this.node = node;
            this.rel = rel;
        }
    }

    public static Map<String, Object> getCommonVectorDbInfo(
            String hostOrKey,
            String collection,
            Map<String, Object> configuration,
            String templateUrl,
            VectorDbHandler handler) {
        Map<String, Object> config = new HashMap<>(configuration);

        Map<String, Object> props = withSystemDb(transaction -> {
            try {
                Label label = Label.label(handler.getLabel());
                Node node = transaction.findNode(label, SystemPropertyKeys.name.name(), hostOrKey);
                return node == null ? Map.of() : node.getAllProperties();
            } catch (Exception e) {
                // Fallback in case of null keys/values
                return Map.of();
            }
        });

        String url = getUrl(hostOrKey, handler, props);
        config.put(BASE_URL_KEY, url);

        Map mappingConfVal = (Map) config.get(MAPPING_KEY);
        if (MapUtils.isEmpty(mappingConfVal)) {
            String mappingStoreVal = (String) props.get(MAPPING_KEY);
            if (mappingStoreVal != null) {
                config.put(MAPPING_KEY, Util.fromJson(mappingStoreVal, Map.class));
            }
        }

        String credentials = (String) props.get(SystemPropertyKeys.credentials.name());
        if (credentials != null) {
            Object credentialsObj = Util.fromJson(credentials, Object.class);

            config = handler.getCredentials(credentialsObj, config);
        }

        String endpoint = String.format(templateUrl, url, collection);
        getEndpoint(config, endpoint);

        return config;
    }

    private static String getUrl(String hostOrKey, VectorDbHandler handler, Map<String, Object> props) {
        if (props.isEmpty()) {
            return handler.getUrl(hostOrKey);
        }
        return (String) props.get(SystemPropertyKeys.host.name());
    }

    public static void checkMappingConf(Map<String, Object> configuration, String procName) {
        if (configuration.containsKey(MAPPING_KEY)) {
            throw new RuntimeException(
                    ERROR_READONLY_MAPPING + "\n" + "Try the equivalent procedure, which is the " + procName);
        }
    }

    /**
     * If the vectorDb is WEAVIATE and endpoint doesn't end with `/vN`, where N is a number,
     * then add `/v1` to the endpoint
     */
    public static String appendVersionUrlIfNeeded(VectorDbHandler.Type type, String host) {
        if (VectorDbHandler.Type.WEAVIATE == type) {
            String regex = ".*(/v\\d+)$";
            if (!host.matches(regex)) {
                host = host + "/v1";
            }
        }
        return host;
    }
}
