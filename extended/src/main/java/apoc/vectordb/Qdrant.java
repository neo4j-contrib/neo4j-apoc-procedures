package apoc.vectordb;

import apoc.Extended;
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

import static apoc.ml.RestAPIConfig.METHOD_KEY;
import static apoc.vectordb.VectorDb.executeRequest;
import static apoc.vectordb.VectorDb.getEmbeddingResultStream;
import static apoc.vectordb.VectorDbUtil.*;
import static apoc.vectordb.VectorDbUtil.VectorDbHandler.Type.QDRANT;

@Extended
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
    @Description("apoc.vectordb.qdrant.createCollection(hostOrKey, collection, similarity, size, $configuration) - Creates a collection, with the name specified in the 2nd parameter, and with the specified `similarity` and `size`")
    public Stream<MapResult> createCollection(@Name("hostOrKey") String hostOrKey,
                                    @Name("collection") String collection,
                                    @Name("similarity") String similarity,
                                    @Name("size") Long size,
                                    @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) throws Exception {
        String url = "%s/collections/%s";
        Map<String, Object> config = getVectorDbInfo(hostOrKey, collection, configuration, url);
        config.putIfAbsent(METHOD_KEY, "PUT");

        Map<String, Object> additionalBodies = Map.of("vectors", Map.of(
                "size", size,
                "distance", similarity
        ));
        RestAPIConfig restAPIConfig = new RestAPIConfig(config, Map.of(), additionalBodies);
        return executeRequest(restAPIConfig, urlAccessChecker)
                .map(v -> (Map<String,Object>) v)
                .map(MapResult::new);
    }
    
    @Procedure("apoc.vectordb.qdrant.deleteCollection")
    @Description("apoc.vectordb.qdrant.deleteCollection(hostOrKey, collection, $configuration) - Deletes a collection with the name specified in the 2nd parameter")
    public Stream<MapResult> deleteCollection(
            @Name("hostOrKey") String hostOrKey,
            @Name("collection") String collection,
            @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) throws Exception {

        String url = "%s/collections/%s";
        Map<String, Object> config = getVectorDbInfo(hostOrKey, collection, configuration, url);
        config.putIfAbsent(METHOD_KEY, "DELETE");

        RestAPIConfig restAPIConfig = new RestAPIConfig(config);
        return executeRequest(restAPIConfig, urlAccessChecker)
                .map(v -> (Map<String,Object>) v)
                .map(MapResult::new);
    }
    
    @Procedure("apoc.vectordb.qdrant.upsert")
    @Description("apoc.vectordb.qdrant.upsert(hostOrKey, collection, vectors, $configuration) - Upserts, in the collection with the name specified in the 2nd parameter, the vectors [{id: 'id', vector: '<vectorDb>', medatada: '<metadata>'}]")
    public Stream<MapResult> upsert(
            @Name("hostOrKey") String hostOrKey,
            @Name("collection") String collection,
            @Name("vectors") List<Map<String, Object>> vectors,
            @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) throws Exception {

        String url = "%s/collections/%s/points";

        Map<String, Object> config = getVectorDbInfo(hostOrKey, collection, configuration, url);
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
                .map(v -> (Map<String,Object>) v)
                .map(MapResult::new);
    }
    
    @Procedure("apoc.vectordb.qdrant.delete")
    @Description("apoc.vectordb.qdrant.delete(hostOrKey, collection, ids, $configuration) - Delete the vectors with the specified `ids`")
    public Stream<MapResult> delete(
            @Name("hostOrKey") String hostOrKey,
            @Name("collection") String collection,
            @Name("vectors") List<Object> ids,
            @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) throws Exception {

        String url = "%s/collections/%s/points/delete";
        Map<String, Object> config = getVectorDbInfo(hostOrKey, collection, configuration, url);
        config.putIfAbsent(METHOD_KEY, "POST");

        Map<String, Object> additionalBodies = Map.of("points", ids);
        RestAPIConfig apiConfig = new RestAPIConfig(config, Map.of(), additionalBodies);
        return executeRequest(apiConfig, urlAccessChecker)
                .map(v -> (Map<String,Object>) v)
                .map(MapResult::new);
    }

    @Procedure(value = "apoc.vectordb.qdrant.get", mode = Mode.SCHEMA)
    @Description("apoc.vectordb.qdrant.get(hostOrKey, collection, ids, $configuration) - Get the vectors with the specified `ids`")
    public Stream<EmbeddingResult> query(@Name("hostOrKey") String hostOrKey,
                                                      @Name("collection") String collection,
                                                      @Name("ids") List<Object> ids,
                                                      @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) throws Exception {
        String url = "%s/collections/%s/points";
        Map<String, Object> config = getVectorDbInfo(hostOrKey, collection, configuration, url);

        VectorEmbeddingConfig apiConfig = QDRANT.get().getEmbedding().fromGet(config, procedureCallContext, ids);
        return getEmbeddingResultStream(apiConfig, procedureCallContext, urlAccessChecker, db, tx);
    }

    @Procedure(value = "apoc.vectordb.qdrant.query", mode = Mode.SCHEMA)
    @Description("apoc.vectordb.qdrant.query(hostOrKey, collection, vector, filter, limit, $configuration) - Retrieve closest vectors the the defined `vector`, `limit` of results,  in the collection with the name specified in the 2nd parameter")
    public Stream<EmbeddingResult> query(@Name("hostOrKey") String hostOrKey,
                                                      @Name("collection") String collection,
                                                      @Name(value = "vector", defaultValue = "[]") List<Double> vector,
                                                      @Name(value = "filter", defaultValue = "{}") Map<String, Object> filter,
                                                      @Name(value = "limit", defaultValue = "10") long limit,
                                                      @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) throws Exception {
        
        String url = "%s/collections/%s/points/search";
        Map<String, Object> config = getVectorDbInfo(hostOrKey, collection, configuration, url);

        VectorEmbeddingConfig apiConfig = QDRANT.get().getEmbedding().fromQuery(config, procedureCallContext, vector, filter, limit, collection);
        return getEmbeddingResultStream(apiConfig, procedureCallContext, urlAccessChecker, db, tx);
    }

    private Map<String, Object> getVectorDbInfo(
            String hostOrKey, String collection, Map<String, Object> configuration, String templateUrl) {
        return getCommonVectorDbInfo(hostOrKey, collection, configuration, templateUrl, QDRANT.get());
    }
}
