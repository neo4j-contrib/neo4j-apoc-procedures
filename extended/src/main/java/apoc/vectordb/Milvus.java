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
import static apoc.vectordb.VectorDbHandler.Type.MILVUS;
import static apoc.vectordb.VectorDbUtil.*;

@Extended
public class Milvus {

    @Context
    public ProcedureCallContext procedureCallContext;

    @Context
    public Transaction tx;

    @Context
    public GraphDatabaseService db;

    @Context
    public URLAccessChecker urlAccessChecker;

    @Procedure("apoc.vectordb.milvus.createCollection")
    @Description("apoc.vectordb.milvus.createCollection(hostOrKey, collection, similarity, size, $configuration) - Creates a collection, with the name specified in the 2nd parameter, and with the specified `similarity` and `size`")
    public Stream<MapResult> createCollection(@Name("hostOrKey") String hostOrKey,
                                              @Name("collection") String collection,
                                              @Name("similarity") String similarity,
                                              @Name("size") Long size,
                                              @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) throws Exception {
        String url = "%s/v2/vectordb/collections/create";
        Map<String, Object> config = getVectorDbInfo(hostOrKey, collection, configuration, url);
        config.putIfAbsent(METHOD_KEY, "POST");

        Map<String, Object> additionalBodies = Map.of("collectionName", collection,
                "dimension", size,
                "metricType", similarity
        );
        RestAPIConfig restAPIConfig = new RestAPIConfig(config, Map.of(), additionalBodies);
        return executeRequest(restAPIConfig, urlAccessChecker)
                .map(v -> (Map<String,Object>) v)
                .map(MapResult::new);
    }

    @Procedure("apoc.vectordb.milvus.deleteCollection")
    @Description("apoc.vectordb.milvus.deleteCollection(hostOrKey, collection, $configuration) - Deletes a collection with the name specified in the 2nd parameter")
    public Stream<MapResult> deleteCollection(
            @Name("hostOrKey") String hostOrKey,
            @Name("collection") String collection,
            @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) throws Exception {

        String url = "%s/v2/vectordb/collections/drop";
        Map<String, Object> config = getVectorDbInfo(hostOrKey, collection, configuration, url);
        config.putIfAbsent(METHOD_KEY, "POST");
        Map<String, Object> additionalBodies = Map.of("collectionName", collection);

        RestAPIConfig restAPIConfig = new RestAPIConfig(config, Map.of(), additionalBodies);
        return executeRequest(restAPIConfig, urlAccessChecker)
                .map(v -> (Map<String,Object>) v)
                .map(MapResult::new);
    }

    @Procedure("apoc.vectordb.milvus.upsert")
    @Description("apoc.vectordb.milvus.upsert(hostOrKey, collection, vectors, $configuration) - Upserts, in the collection with the name specified in the 2nd parameter, the vectors [{id: 'id', vector: '<vectorDb>', medatada: '<metadata>'}]")
    public Stream<MapResult> upsert(
            @Name("hostOrKey") String hostOrKey,
            @Name("collection") String collection,
            @Name("vectors") List<Map<String, Object>> vectors,
            @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) throws Exception {

        String url = "%s/v2/vectordb/entities/upsert";

        Map<String, Object> config = getVectorDbInfo(hostOrKey, collection, configuration, url);
        config.putIfAbsent(METHOD_KEY, "POST");

        List<Map<String, Object>> data = vectors.stream()
                .map(i -> {
                    Map<String, Object> map = new HashMap<>(i);
                    map.putAll((Map) map.remove("metadata"));
                    return map;
                })
                .toList();
        Map<String, Object> additionalBodies = Map.of("data", data,
                "collectionName", collection);
        RestAPIConfig restAPIConfig = new RestAPIConfig(config, Map.of(), additionalBodies);
        return executeRequest(restAPIConfig, urlAccessChecker)
                .map(v -> (Map<String,Object>) v)
                .map(MapResult::new);
    }

    @Procedure("apoc.vectordb.milvus.delete")
    @Description("apoc.vectordb.milvus.delete(hostOrKey, collection, ids, $configuration) - Delete the vectors with the specified `ids`")
    public Stream<MapResult> delete(
            @Name("hostOrKey") String hostOrKey,
            @Name("collection") String collection,
            @Name("vectors") List<Object> ids,
            @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) throws Exception {

        String url = "%s/v2/vectordb/entities/delete";
        Map<String, Object> config = getVectorDbInfo(hostOrKey, collection, configuration, url);
        config.putIfAbsent(METHOD_KEY, "POST");

        String filter = "id in " + ids;
        System.out.println("filter = " + filter);
        Map<String, Object> additionalBodies = Map.of("collectionName", collection, "filter", filter);
        RestAPIConfig apiConfig = new RestAPIConfig(config, Map.of(), additionalBodies);
        return executeRequest(apiConfig, urlAccessChecker)
                .map(v -> (Map<String,Object>) v)
                .map(MapResult::new);
    }

    @Procedure(value = "apoc.vectordb.milvus.get", mode = Mode.WRITE)
    @Description("apoc.vectordb.milvus.get(hostOrKey, collection, ids, $configuration) - Get the vectors with the specified `ids`")
    public Stream<EmbeddingResult> get(@Name("hostOrKey") String hostOrKey,
                                         @Name("collection") String collection,
                                         @Name("ids") List<Object> ids,
                                         @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) throws Exception {
        return getCommon(hostOrKey, collection, ids, configuration, true);
    }

    @Procedure(value = "apoc.vectordb.milvus.getAndUpdate", mode = Mode.WRITE)
    @Description("apoc.vectordb.qdrant.getAndUpdate(hostOrKey, collection, ids, $configuration) - Gets the vectors with the specified `ids`, and optionally creates/updates neo4j entities")
    public Stream<EmbeddingResult> getAndUpdate(@Name("hostOrKey") String hostOrKey,
                                                @Name("collection") String collection,
                                                @Name("ids") List<Object> ids,
                                                @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) throws Exception {
        return getCommon(hostOrKey, collection, ids, configuration, false);
    }

    private Stream<EmbeddingResult> getCommon(String hostOrKey, String collection, List<Object> ids, Map<String, Object> configuration, boolean readOnly) throws Exception {
        String url = "%s/v2/vectordb/entities/get";
        Map<String, Object> config = getVectorDbInfo(hostOrKey, collection, configuration, url);

        if (readOnly) {
            checkMappingConf(configuration, "apoc.vectordb.milvus.getAndUpdate");
        }

        VectorEmbeddingConfig apiConfig = MILVUS.get().getEmbedding().fromGet(config, procedureCallContext, ids, collection);
        return getEmbeddingResultStream(apiConfig, procedureCallContext, urlAccessChecker, tx,
                v -> getMapStream((Map) v));
    }

    @Procedure(value = "apoc.vectordb.milvus.query")
    @Description("apoc.vectordb.milvus.query(hostOrKey, collection, vector, filter, limit, $configuration) - Retrieve closest vectors the the defined `vector`, `limit` of results,  in the collection with the name specified in the 2nd parameter")
    public Stream<EmbeddingResult> query(@Name("hostOrKey") String hostOrKey,
                                         @Name("collection") String collection,
                                         @Name(value = "vector", defaultValue = "[]") List<Double> vector,
                                         @Name(value = "filter", defaultValue = "null") Object filter,
                                         @Name(value = "limit", defaultValue = "10") long limit,
                                         @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) throws Exception {

        return queryCommon(hostOrKey, collection, vector, filter, limit, configuration, true);
    }

    @Procedure(value = "apoc.vectordb.milvus.queryAndUpdate", mode = Mode.WRITE)
    @Description("apoc.vectordb.milvus.queryAndUpdate(hostOrKey, collection, vector, filter, limit, $configuration) - Retrieve closest vectors the the defined `vector`, `limit` of results,  in the collection with the name specified in the 2nd parameter")
    public Stream<EmbeddingResult> queryAndUpdate(@Name("hostOrKey") String hostOrKey,
                                         @Name("collection") String collection,
                                         @Name(value = "vector", defaultValue = "[]") List<Double> vector,
                                         @Name(value = "filter", defaultValue = "null") Object filter,
                                         @Name(value = "limit", defaultValue = "10") long limit,
                                         @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) throws Exception {

        return queryCommon(hostOrKey, collection, vector, filter, limit, configuration, false);
    }

    private Stream<Map> getMapStream(Map v) {
        var data = v.get("data");

        return ((List<Map>) data).stream()
                .map(i -> {
                    var metadata = new HashMap<>(i);
                    metadata.remove("id");
                    metadata.remove("vector");
                    metadata.remove("distance");

                    i.put("metadata", metadata);

                    return i;
                });
    }

    private Stream<EmbeddingResult> queryCommon(String hostOrKey, String collection, List<Double> vector, Object filter, long limit, Map<String, Object> configuration, boolean readOnly) throws Exception {
        String url = "%s/v2/vectordb/entities/search";
        Map<String, Object> config = getVectorDbInfo(hostOrKey, collection, configuration, url);

        if (readOnly) {
            checkMappingConf(configuration, "apoc.vectordb.milvus.queryAndUpdate");
        }
        
        VectorEmbeddingConfig apiConfig = MILVUS.get().getEmbedding().fromQuery(config, procedureCallContext, vector, filter, limit, collection);
        return getEmbeddingResultStream(apiConfig, procedureCallContext, urlAccessChecker, tx,
                v -> getMapStream((Map) v));
    }

    private Map<String, Object> getVectorDbInfo(
            String hostOrKey, String collection, Map<String, Object> configuration, String templateUrl) {
        return getCommonVectorDbInfo(hostOrKey, collection, configuration, templateUrl, MILVUS.get());
    }
}
