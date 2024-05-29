package apoc.vectordb;

import apoc.Extended;
import apoc.ml.RestAPIConfig;
import apoc.result.MapResult;
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
import static apoc.vectordb.VectorDbHandler.Type.PINECONE;
import static apoc.vectordb.VectorDbUtil.getCommonVectorDbInfo;

@Extended
public class Pinecone {
    public static final VectorDbHandler DB_HANDLER = PINECONE.get();
    
    @Context
    public ProcedureCallContext procedureCallContext;

    @Context
    public Transaction tx;

    @Context
    public GraphDatabaseService db;

    @Context
    public URLAccessChecker urlAccessChecker;

    @Procedure("apoc.vectordb.pinecone.createCollection")
    @Description("apoc.vectordb.pinecone.createCollection(hostOrKey, collection, similarity, size, $configuration) - Creates a collection, with the name specified in the 2nd parameter, and with the specified `similarity` and `size`")
    public Stream<MapResult> createCollection(@Name("hostOrKey") String hostOrKey,
                                              @Name("collection") String collection,
                                              @Name("similarity") String similarity,
                                              @Name("size") Long size,
                                              @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) throws Exception {
        String url = "%s/indexes";
        Map<String, Object> config = getVectorDbInfo(hostOrKey, collection, configuration, url);
        config.putIfAbsent(METHOD_KEY, "POST");

        Map<String, Object> additionalBodies = Map.of(
                "name", collection,
                "dimension", size,
                "metric", similarity
        );
        RestAPIConfig restAPIConfig = new RestAPIConfig(config, Map.of(), additionalBodies);
        return executeRequest(restAPIConfig, urlAccessChecker)
                .map(v -> (Map<String,Object>) v)
                .map(MapResult::new);
    }

    @Procedure("apoc.vectordb.pinecone.deleteCollection")
    @Description("apoc.vectordb.pinecone.deleteCollection(hostOrKey, collection, $configuration) - Deletes a collection with the name specified in the 2nd parameter")
    public Stream<MapResult> deleteCollection(
            @Name("hostOrKey") String hostOrKey,
            @Name("collection") String collection,
            @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) throws Exception {

        String url = "%s/indexes/%s";
        Map<String, Object> config = getVectorDbInfo(hostOrKey, collection, configuration, url);
        config.putIfAbsent(METHOD_KEY, "DELETE");

        RestAPIConfig restAPIConfig = new RestAPIConfig(config);
        return executeRequest(restAPIConfig, urlAccessChecker)
                .map(v -> (Map<String,Object>) v)
                .map(MapResult::new);
    }

    @Procedure("apoc.vectordb.pinecone.upsert")
    @Description("apoc.vectordb.pinecone.upsert(hostOrKey, collection, vectors, $configuration) - Upserts, in the collection with the name specified in the 2nd parameter, the vectors [{id: 'id', vector: '<vectorDb>', medatada: '<metadata>'}]")
    public Stream<MapResult> upsert(
            @Name("hostOrKey") String hostOrKey,
            @Name("collection") String collection,
            @Name("vectors") List<Map<String, Object>> vectors,
            @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) throws Exception {

        String url = "%s/vectors/upsert";

        Map<String, Object> config = getVectorDbInfo(hostOrKey, collection, configuration, url);
        config.putIfAbsent(METHOD_KEY, "POST");

        vectors = vectors.stream()
                .map(i -> {
                    Map<String, Object> map = new HashMap<>(i);
                    map.putIfAbsent("values", map.remove("vector"));
                    return map;
                })
                .toList();
        
        Map<String, Object> additionalBodies = Map.of("vectors", vectors);
        RestAPIConfig restAPIConfig = new RestAPIConfig(config, Map.of(), additionalBodies);
        return executeRequest(restAPIConfig, urlAccessChecker)
                .map(v -> (Map<String,Object>) v)
                .map(MapResult::new);
    }

    @Procedure("apoc.vectordb.pinecone.delete")
    @Description("apoc.vectordb.pinecone.delete(hostOrKey, collection, ids, $configuration) - Delete the vectors with the specified `ids`")
    public Stream<MapResult> delete(
            @Name("hostOrKey") String hostOrKey,
            @Name("collection") String collection,
            @Name("vectors") List<Object> ids,
            @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) throws Exception {

        String url = "%s/vectors/delete";
        Map<String, Object> config = getVectorDbInfo(hostOrKey, collection, configuration, url);
        config.putIfAbsent(METHOD_KEY, "POST");

        Map<String, Object> additionalBodies = Map.of("ids", ids);
        RestAPIConfig apiConfig = new RestAPIConfig(config, Map.of(), additionalBodies);
        return executeRequest(apiConfig, urlAccessChecker)
                .map(v -> (Map<String,Object>) v)
                .map(MapResult::new);
    }

    @Procedure(value = "apoc.vectordb.pinecone.get")
    @Description("apoc.vectordb.pinecone.get(hostOrKey, collection, ids, $configuration) - Get the vectors with the specified `ids`")
    public Stream<VectorDbUtil.EmbeddingResult> get(@Name("hostOrKey") String hostOrKey,
                                                      @Name("collection") String collection,
                                                      @Name("ids") List<Object> ids,
                                                      @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) throws Exception {
        return getCommon(hostOrKey, collection, ids, configuration, false);
    }

    @Procedure(value = "apoc.vectordb.pinecone.getAndUpdate", mode = Mode.WRITE)
    @Description("apoc.vectordb.pinecone.getAndUpdate(hostOrKey, collection, ids, $configuration) - Get the vectors with the specified `ids`")
    public Stream<VectorDbUtil.EmbeddingResult> getAndUpdate(@Name("hostOrKey") String hostOrKey,
                                                      @Name("collection") String collection,
                                                      @Name("ids") List<Object> ids,
                                                      @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) throws Exception {
        return getCommon(hostOrKey, collection, ids, configuration, true);
    }

    private Stream<VectorDbUtil.EmbeddingResult> getCommon(String hostOrKey, String collection, List<Object> ids, Map<String, Object> configuration, boolean updateMode) throws Exception {
        String url = "%s/vectors/fetch";
        Map<String, Object> config = getVectorDbInfo(hostOrKey, collection, configuration, url);
        
        VectorEmbeddingConfig conf = DB_HANDLER.getEmbedding().fromGet(config, procedureCallContext, ids, collection);
        conf.getMapping().setUpdateMode(updateMode);
        
        return getEmbeddingResultStream(conf, procedureCallContext, urlAccessChecker, tx,
                v -> {
                    Object vectors = ((Map) v).get("vectors");
                    return ((Map) vectors).values()
                            .stream();
                });
    }

    @Procedure(value = "apoc.vectordb.pinecone.query")
    @Description("apoc.vectordb.pinecone.query(hostOrKey, collection, vector, filter, limit, $configuration) - Retrieve closest vectors the the defined `vector`, `limit` of results,  in the collection with the name specified in the 2nd parameter")
    public Stream<VectorDbUtil.EmbeddingResult> query(@Name("hostOrKey") String hostOrKey,
                                                      @Name("collection") String collection,
                                                      @Name(value = "vector", defaultValue = "[]") List<Double> vector,
                                                      @Name(value = "filter", defaultValue = "{}") Map<String, Object> filter,
                                                      @Name(value = "limit", defaultValue = "10") long limit,
                                                      @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) throws Exception {
        return queryCommon(hostOrKey, collection, vector, filter, limit, configuration, false);
    }

    @Procedure(value = "apoc.vectordb.pinecone.queryAndUpdate", mode = Mode.WRITE)
    @Description("apoc.vectordb.pinecone.queryAndUpdate(hostOrKey, collection, vector, filter, limit, $configuration) - Retrieve closest vectors the the defined `vector`, `limit` of results,  in the collection with the name specified in the 2nd parameter")
    public Stream<VectorDbUtil.EmbeddingResult> queryAndUpdate(@Name("hostOrKey") String hostOrKey,
                                                      @Name("collection") String collection,
                                                      @Name(value = "vector", defaultValue = "[]") List<Double> vector,
                                                      @Name(value = "filter", defaultValue = "{}") Map<String, Object> filter,
                                                      @Name(value = "limit", defaultValue = "10") long limit,
                                                      @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) throws Exception {
        return queryCommon(hostOrKey, collection, vector, filter, limit, configuration, true);
    }

    private Stream<VectorDbUtil.EmbeddingResult> queryCommon(String hostOrKey, String collection, List<Double> vector, Map<String, Object> filter, long limit, Map<String, Object> configuration, boolean updateMode) throws Exception {
        String url = "%s/query";
        Map<String, Object> config = getVectorDbInfo(hostOrKey, collection, configuration, url);

        VectorEmbeddingConfig conf = DB_HANDLER.getEmbedding().fromQuery(config, procedureCallContext, vector, filter, limit, collection);
        conf.getMapping().setUpdateMode(updateMode);
        
        return getEmbeddingResultStream(conf, procedureCallContext, urlAccessChecker, tx,
                v -> {
                    Map map = (Map) v;
                    return ((List) map.get("matches"))
                            .stream();
                });
    }

    private Map<String, Object> getVectorDbInfo(
            String hostOrKey, String collection, Map<String, Object> configuration, String templateUrl) {
        return getCommonVectorDbInfo(hostOrKey, collection, configuration, templateUrl, DB_HANDLER);
    }
}
