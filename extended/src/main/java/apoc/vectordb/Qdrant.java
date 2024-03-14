package apoc.vectordb;

import apoc.ml.RestAPIConfig;
import apoc.result.MapResult;
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

import static apoc.ml.RestAPIConfig.ENDPOINT_KEY;
import static apoc.ml.RestAPIConfig.METHOD_KEY;
import static apoc.vectordb.VectorDb.executeRequest;
import static apoc.vectordb.VectorDb.getEmbeddingResultStream;
import static apoc.vectordb.VectorDbUtil.getEndpoint;
import static apoc.vectordb.VectorEmbedding.Type.QDRANT;
import static apoc.vectordb.VectorEmbeddingConfig.EMBEDDING_KEY;

public class Qdrant {

    @Context
    public ProcedureCallContext procedureCallContext;

    @Context
    public Transaction tx;

    @Context
    public GraphDatabaseService db;
    
    @Context
    public URLAccessChecker urlAccessChecker;

    @Procedure("apoc.vectordb.qdrant.createCollection")
    @Description("apoc.vectordb.qdrant.createCollection(hostOrKey, collection, similarity, size, $config)")
    public Stream<MapResult> createCollection(@Name("hostOrKey") String hostOrKey,
                                    @Name("collection") String collection,
                                    @Name("similarity") String similarity,
                                    @Name("size") Long size,
                                    @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) throws Exception {
        var config = new HashMap<>(configuration);

        String qdrantUrl = getQdrantUrl(hostOrKey);
        String endpoint = "%s/collections/%s".formatted(qdrantUrl, collection);
        getEndpoint(config, endpoint);
        config.putIfAbsent(METHOD_KEY, "PUT");

        Map<String, Object> additionalBodies = Map.of("vectors", Map.of(
                "size", size,
                "distance", similarity
        ));
        RestAPIConfig restAPIConfig = new RestAPIConfig(config, Map.of(), additionalBodies);
        return executeRequest(restAPIConfig, urlAccessChecker)
                .map(v -> (Map<String,Object>)v)
                .map(MapResult::new);
    }
    
    @Procedure("apoc.vectordb.qdrant.deleteCollection")
    @Description("apoc.vectordb.qdrant.deleteCollection(hostOrKey, collection, $config)")
    public Stream<MapResult> deleteCollection(
            @Name("hostOrKey") String hostOrKey,
            @Name("collection") String collection,
            @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) throws Exception {

        var config = new HashMap<>(configuration);

        String qdrantUrl = getQdrantUrl(hostOrKey);
        String endpoint = "%s/collections/%s".formatted(qdrantUrl, collection);
        getEndpoint(config, endpoint);
        config.putIfAbsent(METHOD_KEY, "DELETE");

        RestAPIConfig restAPIConfig = new RestAPIConfig(config);
        return executeRequest(restAPIConfig, urlAccessChecker)
                .map(v -> (Map<String,Object>)v)
                .map(MapResult::new);
    }
    
    @Procedure("apoc.vectordb.qdrant.upsert")
    @Description("apoc.vectordb.qdrant.upsert(hostOrKey, collection, vectors, $config)")
    public Stream<MapResult> upsert(
            @Name("hostOrKey") String hostOrKey,
            @Name("collection") String collection,
            @Name("vectors") List<Map<String, Object>> vectors,
            @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) throws Exception {

        var config = new HashMap<>(configuration);

        String qdrantUrl = getQdrantUrl(hostOrKey);
        String endpoint = "%s/collections/%s/points".formatted(qdrantUrl, collection);
        getEndpoint(config, endpoint);
        config.putIfAbsent(METHOD_KEY, "PUT");

        List<Map<String, Object>> point = vectors.stream()
                .map(i -> {
                    Map<String, Object> map = new HashMap<>(i);
                    map.putIfAbsent("vector", map.remove("vector"));
                    map.putIfAbsent("payload", map.remove("metadata"));
                    return map;
                })
                .toList();
        Map<String, Object> additionalBodies = Map.of("points", point);
        RestAPIConfig restAPIConfig = new RestAPIConfig(config, Map.of(), additionalBodies);
        return executeRequest(restAPIConfig, urlAccessChecker)
                .map(v -> (Map<String,Object>)v)
                .map(MapResult::new);
    }
    
    @Procedure("apoc.vectordb.qdrant.delete")
    @Description("apoc.vectordb.qdrant.delete(hostOrKey, collection, ids, $config)")
    public Stream<MapResult> delete(
            @Name("hostOrKey") String hostOrKey,
            @Name("collection") String collection,
            @Name("vectors") List<Object> ids,
            @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) throws Exception {

        var config = new HashMap<>(configuration);

        String qdrantUrl = getQdrantUrl(hostOrKey);
        String endpoint = "%s/collections/%s/points/delete".formatted(qdrantUrl, collection);
        getEndpoint(config, endpoint);
        config.putIfAbsent(METHOD_KEY, "POST");

        Map<String, Object> additionalBodies = Map.of("points", ids);
        RestAPIConfig apiConfig = new RestAPIConfig(config, Map.of(), additionalBodies);
        return executeRequest(apiConfig, urlAccessChecker)
                .map(v -> (Map<String,Object>)v)
                .map(MapResult::new);
    }

    @Procedure(value = "apoc.vectordb.qdrant.get", mode = Mode.SCHEMA)
    @Description("apoc.vectordb.qdrant.get(hostOrKey, collection, ids, $config)")
    public Stream<VectorDbUtil.EmbeddingResult> query(@Name("hostOrKey") String hostOrKey,
                                                      @Name("collection") String collection,
                                                      @Name("ids") List<Object> ids,
                                                      @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) throws Exception {
        var config = new HashMap<>(configuration);
        
        String qdrantUrl = getQdrantUrl(hostOrKey);
        String endpoint = "%s/collections/%s/points".formatted(qdrantUrl, collection);
        getEndpoint(config, endpoint);

        VectorEmbeddingConfig apiConfig = QDRANT.get().fromGet(config, procedureCallContext, ids);
        return getEmbeddingResultStream(apiConfig, procedureCallContext, urlAccessChecker, db, tx);
    }

    @Procedure(value = "apoc.vectordb.qdrant.query", mode = Mode.SCHEMA)
    @Description("apoc.vectordb.qdrant.query(hostOrKey, collection, vector, filter, limit, $config)")
    public Stream<VectorDbUtil.EmbeddingResult> query(@Name("hostOrKey") String hostOrKey,
                                                      @Name("collection") String collection,
                                                      @Name(value = "vector", defaultValue = "[]") List<Double> vector,
                                                      @Name(value = "filter", defaultValue = "{}") Map<String, Object> filter,
                                                      @Name(value = "limit", defaultValue = "10") long limit,
                                                      @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) throws Exception {
        
        var config = new HashMap<>(configuration);
        
        String qdrantUrl = getQdrantUrl(hostOrKey);
        String endpoint = "%s/collections/%s/points/search".formatted(qdrantUrl, collection);
        getEndpoint(config, endpoint);

        VectorEmbeddingConfig apiConfig = QDRANT.get().fromQuery(config, procedureCallContext, vector, filter, limit);
        return getEmbeddingResultStream(apiConfig, procedureCallContext, urlAccessChecker, db, tx);
    }

    protected String getQdrantUrl(String hostOrKey) {
        return new UrlResolver("http", "localhost", 6333).getUrl("qdrant", hostOrKey);
    }
}
