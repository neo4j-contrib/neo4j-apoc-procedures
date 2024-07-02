package apoc.vectordb;

import apoc.Extended;
import apoc.ml.RestAPIConfig;
import apoc.result.ListResult;
import apoc.result.MapResult;
import apoc.util.CollectionUtils;
import apoc.util.UrlResolver;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.security.URLAccessChecker;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static apoc.ml.RestAPIConfig.METHOD_KEY;
import static apoc.util.Util.map;
import static apoc.vectordb.VectorDb.executeRequest;
import static apoc.vectordb.VectorDb.getEmbeddingResult;
import static apoc.vectordb.VectorDb.getEmbeddingResultStream;
import static apoc.vectordb.VectorDbHandler.Type.WEAVIATE;
import static apoc.vectordb.VectorDbUtil.*;
import static apoc.vectordb.VectorEmbeddingConfig.DEFAULT_ERRORS;

@Extended
public class Weaviate {
    public static final VectorDbHandler DB_HANDLER = WEAVIATE.get();
    
    @Context
    public ProcedureCallContext procedureCallContext;

    @Context
    public Transaction tx;

    @Context
    public GraphDatabaseService db;

    @Context
    public URLAccessChecker urlAccessChecker;

    @Procedure("apoc.vectordb.weaviate.createCollection")
    @Description("apoc.vectordb.weaviate.createCollection(hostOrKey, collection, similarity, size, $configuration) - Creates a collection, with the name specified in the 2nd parameter, and with the specified `similarity` and `size`")
    public Stream<MapResult> createCollection(@Name("hostOrKey") String hostOrKey,
                                              @Name("collection") String collection,
                                              @Name("similarity") String similarity,
                                              @Name("size") Long size,
                                              @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) throws Exception {
        var config = getVectorDbInfo(hostOrKey, collection, configuration, "%s/schema");
        config.putIfAbsent(METHOD_KEY, "POST");

        Map<String, Object> additionalBodies = Map.of("class", collection,
                "vectorIndexConfig", Map.of("distance", similarity, "size", size)
        );

        RestAPIConfig restAPIConfig = new RestAPIConfig(config, Map.of(), additionalBodies);

        return executeRequest(restAPIConfig, urlAccessChecker)
                .map(v -> (Map<String,Object>) v)
                .map(MapResult::new);
    }

    @Procedure("apoc.vectordb.weaviate.deleteCollection")
    @Description("apoc.vectordb.weaviate.deleteCollection(hostOrKey, collection, $configuration) - Deletes a collection with the name specified in the 2nd parameter")
    public Stream<MapResult> deleteCollection(
            @Name("hostOrKey") String hostOrKey,
            @Name("collection") String collection,
            @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) throws Exception {
        var config = getVectorDbInfo(hostOrKey, collection, configuration, "%s/schema/%s");
        config.putIfAbsent(METHOD_KEY, "DELETE");

        RestAPIConfig restAPIConfig = new RestAPIConfig(config);
        return executeRequest(restAPIConfig, urlAccessChecker)
                .map(v -> (Map<String,Object>) v)
                .map(MapResult::new);
    }


    @Procedure("apoc.vectordb.weaviate.upsert")
    @Description("apoc.vectordb.weaviate.upsert(hostOrKey, collection, vectors, $configuration) - Upserts, in the collection with the name specified in the 2nd parameter, the vectors [{id: 'id', vector: '<vectorDb>', medatada: '<metadata>'}]")
    public Stream<MapResult> upsert(
            @Name("hostOrKey") String hostOrKey,
            @Name("collection") String collection,
            @Name("vectors") List<Map<String, Object>> vectors,
            @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) throws Exception {
        var config = getVectorDbInfo(hostOrKey, collection, configuration, "%s/objects");
        config.putIfAbsent(METHOD_KEY, "POST");

        Map<String, Object> body = new HashMap<>();
        body.put("class", collection);
        RestAPIConfig restAPIConfig = new RestAPIConfig(config, Map.of(), body);
        
        return vectors.stream()
                .flatMap(vector -> {
                    try {
                        Map<String, Object> configBody = new HashMap<>(restAPIConfig.getBody());
                        configBody.putAll(vector);
                        configBody.put("properties", vector.remove("metadata"));
                        restAPIConfig.setBody(configBody);
                        
                        Stream<Object> objectStream = executeRequest(restAPIConfig, urlAccessChecker);
                        return objectStream;
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                })
                .map(v -> (Map<String, Object>) v)
                .map(MapResult::new);
    }

    @Procedure(value = "apoc.vectordb.weaviate.delete")
    @Description("apoc.vectordb.weaviate.delete(hostOrKey, collection, ids, $configuration) - Deletes the vectors with the specified `ids`")
    public Stream<ListResult> delete(@Name("hostOrKey") String hostOrKey,
                                     @Name("collection") String collection,
                                     @Name("ids") List<Object> ids,
                                     @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) throws Exception {
        var config = getVectorDbInfo(hostOrKey, collection, configuration, "%s/schema");
        config.putIfAbsent(METHOD_KEY, "DELETE");

        RestAPIConfig restAPIConfig = new RestAPIConfig(config, map(), map());

        List<Object> objects = ids.stream()
                .peek(id -> {
                    String endpoint = "%s/objects/%s/%s".formatted(restAPIConfig.getBaseUrl(), collection, id);
                    restAPIConfig.setEndpoint(endpoint);
                    try {
                        executeRequest(restAPIConfig, urlAccessChecker);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                })
                .toList();
        return Stream.of(new ListResult(objects));
    }

    @Procedure(value = "apoc.vectordb.weaviate.getAndUpdate", mode = Mode.WRITE)
    @Description("apoc.vectordb.weaviate.getAndUpdate(hostOrKey, collection, ids, $configuration) - Gets the vectors with the specified `ids`")
    public Stream<EmbeddingResult> getAndUpdate(@Name("hostOrKey") String hostOrKey,
                                                      @Name("collection") String collection,
                                                      @Name("ids") List<Object> ids,
                                                      @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) {
        return getCommon(hostOrKey, collection, ids, configuration);
    }

    @Procedure(value = "apoc.vectordb.weaviate.get")
    @Description("apoc.vectordb.weaviate.get(hostOrKey, collection, ids, $configuration) - Gets the vectors with the specified `ids`")
    public Stream<EmbeddingResult> get(@Name("hostOrKey") String hostOrKey,
                                                      @Name("collection") String collection,
                                                      @Name("ids") List<Object> ids,
                                                      @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) {
        setReadOnlyMappingMode(configuration);
        return getCommon(hostOrKey, collection, ids, configuration);
    }

    private Stream<EmbeddingResult> getCommon(String hostOrKey, String collection, List<Object> ids, Map<String, Object> configuration) {
        Map<String, Object> config = getVectorDbInfo(hostOrKey, collection, configuration, "%s/schema");
        
        
        /**
         * TODO: we put method: null as a workaround, it should be "GET": https://weaviate.io/developers/weaviate/api/rest#tag/objects/get/objects/{className}/{id}
         * Since with `method: GET` the {@link apoc.util.Util#openUrlConnection(URL, Map)} has a `setChunkedStreamingMode`
         * that makes the request to respond with an error 405 Method Not Allowed 
         */
        config.putIfAbsent(METHOD_KEY, null);

        List<String> fields = procedureCallContext.outputFields().toList();
        VectorEmbeddingConfig conf = DB_HANDLER.getEmbedding().fromGet(config, procedureCallContext, ids, collection);
        
        boolean hasEmbedding = fields.contains("vector") && conf.isAllResults();
        boolean hasMetadata = fields.contains("metadata");
        VectorMappingConfig mapping = conf.getMapping();

        String suffix = hasEmbedding ? "?include=vector" : "";

        return ids.stream()
                .flatMap(id -> {
                    String endpoint = "%s/objects/%s/%s".formatted(conf.getApiConfig().getBaseUrl(), collection, id) + suffix;
                    conf.getApiConfig().setEndpoint(endpoint);
                    try {
                        return executeRequest(conf.getApiConfig(), urlAccessChecker)
                                .map(v -> (Map) v)
                                .map(m -> getEmbeddingResult(conf, tx, hasEmbedding, hasMetadata, mapping, m));
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    @Procedure(value = "apoc.vectordb.weaviate.query")
    @Description("apoc.vectordb.weaviate.query(hostOrKey, collection, vector, filter, limit, $configuration) - Retrieves closest vectors from the defined `vector`, `limit` of results, in the collection with the name specified in the 2nd parameter")
    public Stream<EmbeddingResult> query(@Name("hostOrKey") String hostOrKey,
                                                      @Name("collection") String collection,
                                                      @Name(value = "vector", defaultValue = "[]") List<Double> vector,
                                                      @Name(value = "filter", defaultValue = "null") Object filter,
                                                      @Name(value = "limit", defaultValue = "10") long limit,
                                                      @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) throws Exception {
        setReadOnlyMappingMode(configuration);
        return queryCommon(hostOrKey, collection, vector, filter, limit, configuration);
    }


    @Procedure(value = "apoc.vectordb.weaviate.queryAndUpdate", mode = Mode.WRITE)
    @Description("apoc.vectordb.weaviate.queryAndUpdate(hostOrKey, collection, vector, filter, limit, $configuration) - Retrieves closest vectors from the defined `vector`, `limit` of results, in the collection with the name specified in the 2nd parameter")
    public Stream<EmbeddingResult> queryAndUpdate(@Name("hostOrKey") String hostOrKey,
                                                      @Name("collection") String collection,
                                                      @Name(value = "vector", defaultValue = "[]") List<Double> vector,
                                                      @Name(value = "filter", defaultValue = "null") Object filter,
                                                      @Name(value = "limit", defaultValue = "10") long limit,
                                                      @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) throws Exception {
        return queryCommon(hostOrKey, collection, vector, filter, limit, configuration);
    }

    private Stream<EmbeddingResult> queryCommon(String hostOrKey, String collection, List<Double> vector, Object filter, long limit, Map<String, Object> configuration) throws Exception {
        Map<String, Object> config = getVectorDbInfo(hostOrKey, collection, configuration, "%s/graphql");

        VectorEmbeddingConfig conf = DB_HANDLER.getEmbedding().fromQuery(config, procedureCallContext, vector, filter, limit, collection);
        
        return getEmbeddingResultStream(conf, procedureCallContext, urlAccessChecker, tx,
                v -> {
                    Map<String, Map> mapResult = (Map<String, Map>) v;
                    List<Map> errors = (List<Map>) mapResult.get("errors");
                    if (CollectionUtils.isNotEmpty(errors)) {
                        Map map = new HashMap<>();
                        map.put(DEFAULT_ERRORS, errors);
                        return Stream.of(map);
                    }
                    Object getValue = mapResult.get("data").get("Get");
                    Object collectionValue = ((Map) getValue).get(collection);
                    return ((List<Map>) collectionValue).stream()
                            .map(i -> {
                                Map additional = (Map) i.remove("_additional");

                                Map map = new HashMap<>();
                                map.put(conf.getMetadataKey(), i);
                                map.put(conf.getScoreKey(), additional.get("distance"));
                                map.put(conf.getIdKey(), additional.get("id"));
                                map.put(conf.getVectorKey(), additional.get("vector"));
                                return map;
                            });
                }
        );
    }

    private Map<String, Object> getVectorDbInfo(String hostOrKey, String collection, Map<String, Object> configuration, String templateUrl) {
        return getCommonVectorDbInfo(hostOrKey, collection, configuration, templateUrl, DB_HANDLER);
    }

    protected String getWeaviateUrl(String hostOrKey) {
        String baseUrl = new UrlResolver("http", "localhost", 8000)
                .getUrl("weaviate", hostOrKey);
        return baseUrl + "/v1";
    }
}
