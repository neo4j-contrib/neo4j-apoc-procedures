/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package apoc.vectordb;

import static apoc.ml.RestAPIConfig.BASE_URL_KEY;
import static apoc.ml.RestAPIConfig.BODY_KEY;
import static apoc.ml.RestAPIConfig.ENDPOINT_KEY;
import static apoc.ml.RestAPIConfig.METHOD_KEY;
import static apoc.util.SystemDbUtil.withSystemDb;
import static apoc.vectordb.VectorEmbeddingConfig.FIELDS_KEY;
import static apoc.vectordb.VectorEmbeddingConfig.MAPPING_KEY;
import static apoc.vectordb.VectorEmbeddingConfig.METADATA_KEY;
import static apoc.vectordb.VectorMappingConfig.MODE_KEY;
import static apoc.vectordb.VectorMappingConfig.MappingMode.READ_ONLY;
import static apoc.vectordb.VectorMappingConfig.NO_FIELDS_ERROR_MSG;

import apoc.SystemPropertyKeys;
import apoc.util.Util;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
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

        /**
         *
         */
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

    /**
     * Get vector configuration from config. parameter and system db database
     */
    public static Map<String, Object> getCommonVectorDbInfo(
            String hostOrKey,
            String collection,
            Map<String, Object> configuration,
            String templateUrl,
            VectorDbHandler handler) {
        Map<String, Object> config = new HashMap<>(configuration);

        Map<String, Object> systemDbProps = getSystemDbProps(hostOrKey, handler);

        String baseUrl = getBaseUrl(hostOrKey, handler, config, systemDbProps);

        getMapping(config, systemDbProps);

        config = getCredentialsFromSystemDb(handler, config, systemDbProps);

        // endpoint creation
        String endpoint = String.format(templateUrl, baseUrl, collection);
        getEndpoint(config, endpoint);

        return config;
    }

    /**
     * Retrieve, if exists, the properties stored via `apoc.vectordb.configure` procedure
     */
    private static Map<String, Object> getSystemDbProps(String hostOrKey, VectorDbHandler handler) {
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
        return props;
    }

    /**
     * Retrieve, if exists, the mapping stored via `apoc.vectordb.configure` procedure or via configuration parameter with key `mapping`
     */
    private static void getMapping(Map<String, Object> config, Map<String, Object> props) {
        Map mappingConfVal = (Map) config.get(MAPPING_KEY);
        if (MapUtils.isEmpty(mappingConfVal)) {
            String mappingStoreVal = (String) props.get(MAPPING_KEY);
            if (mappingStoreVal != null) {
                config.put(MAPPING_KEY, Util.fromJson(mappingStoreVal, Map.class));
            }
        }
    }

    private static Map<String, Object> getCredentialsFromSystemDb(
            VectorDbHandler handler, Map<String, Object> config, Map<String, Object> props) {
        String credentials = (String) props.get(SystemPropertyKeys.credentials.name());
        if (credentials != null) {
            Object credentialsObj = Util.fromJson(credentials, Object.class);

            config = handler.getCredentials(credentialsObj, config);
        }
        return config;
    }

    private static String getBaseUrl(
            String hostOrKey, VectorDbHandler handler, Map<String, Object> config, Map<String, Object> props) {
        String url = getUrl(hostOrKey, handler, props);
        config.put(BASE_URL_KEY, url);
        return url;
    }

    private static String getUrl(String hostOrKey, VectorDbHandler handler, Map<String, Object> props) {
        if (props.isEmpty()) {
            return handler.getUrl(hostOrKey);
        }
        return (String) props.get(SystemPropertyKeys.host.name());
    }

    public static void setReadOnlyMappingMode(Map<String, Object> configuration) {
        Map<String, Object> mappingConf =
                (Map<String, Object>) configuration.getOrDefault(MAPPING_KEY, new HashMap<>());
        mappingConf.put(MODE_KEY, READ_ONLY.toString());
    }

    /**
     * The "method" should be "GET", but is null as a workaround.
     * Since with `method: POST` the {@link Util#openUrlConnection(URL, Map)} has a `setChunkedStreamingMode`
     * that makes the request to respond `405: Method Not Allowed` even if {@link HttpURLConnection#getRequestMethod()} is "GET".
     * In any case, by putting `body: null`, the request is still in GET  by default
     */
    public static void methodAndPayloadNull(Map<String, Object> config) {
        config.put(METHOD_KEY, null);
        config.put(BODY_KEY, null);
    }

    public static List addMetadataKeyToFields(Map<String, Object> config) {
        List listFields = (List) config.getOrDefault(FIELDS_KEY, new ArrayList<>());

        Map<String, Object> mapping = (Map<String, Object>) config.get(MAPPING_KEY);

        String metadataKey = mapping == null ? null : (String) mapping.get(METADATA_KEY);

        if (CollectionUtils.isEmpty(listFields)) {

            if (StringUtils.isEmpty(metadataKey)) {
                throw new RuntimeException(NO_FIELDS_ERROR_MSG);
            }
            listFields.add(metadataKey);
        }

        return listFields;
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
