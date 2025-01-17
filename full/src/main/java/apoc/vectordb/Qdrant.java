package apoc.vectordb;

import static apoc.ml.RestAPIConfig.METHOD_KEY;
import static apoc.vectordb.VectorDb.executeRequest;
import static apoc.vectordb.VectorDb.getEmbeddingResultStream;
import static apoc.vectordb.VectorDbHandler.Type.QDRANT;
import static apoc.vectordb.VectorDbUtil.*;

import apoc.Extended;
import apoc.ml.RestAPIConfig;
import apoc.result.MapResult;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

@Extended
public class Qdrant {
    public static final VectorDbHandler DB_HANDLER = QDRANT.get();

    @Context
    public ProcedureCallContext procedureCallContext;

    @Context
    public Transaction tx;

    @Context
    public GraphDatabaseService db;

    @Procedure("apoc.vectordb.qdrant.info")
    @Description(
            "apoc.vectordb.qdrant.info(hostOrKey, collection, $configuration) - Get information about the specified existing collection or throws an error if it does not exist")
    public Stream<MapResult> info(
            @Name("hostOrKey") String hostOrKey,
            @Name("collection") String collection,
            @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration)
            throws Exception {
        String url = "%s/collections/%s";
        Map<String, Object> config = getVectorDbInfo(hostOrKey, collection, configuration, url);

        methodAndPayloadNull(config);

        RestAPIConfig restAPIConfig = new RestAPIConfig(config, Map.of(), Map.of());

        return executeRequest(restAPIConfig).map(v -> (Map<String, Object>) v).map(MapResult::new);
    }

    @Procedure("apoc.vectordb.qdrant.createCollection")
    @Description(
            "apoc.vectordb.qdrant.createCollection(hostOrKey, collection, similarity, size, $configuration) - Creates a collection, with the name specified in the 2nd parameter, and with the specified `similarity` and `size`")
    public Stream<MapResult> createCollection(
            @Name("hostOrKey") String hostOrKey,
            @Name("collection") String collection,
            @Name("similarity") String similarity,
            @Name("size") Long size,
            @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration)
            throws Exception {
        String url = "%s/collections/%s";
        Map<String, Object> config = getVectorDbInfo(hostOrKey, collection, configuration, url);
        config.putIfAbsent(METHOD_KEY, "PUT");

        Map<String, Object> additionalBodies = Map.of(
                "vectors",
                Map.of(
                        "size", size,
                        "distance", similarity));
        RestAPIConfig restAPIConfig = new RestAPIConfig(config, Map.of(), additionalBodies);
        return executeRequest(restAPIConfig).map(v -> (Map<String, Object>) v).map(MapResult::new);
    }

    @Procedure("apoc.vectordb.qdrant.deleteCollection")
    @Description(
            "apoc.vectordb.qdrant.deleteCollection(hostOrKey, collection, $configuration) - Deletes a collection with the name specified in the 2nd parameter")
    public Stream<MapResult> deleteCollection(
            @Name("hostOrKey") String hostOrKey,
            @Name("collection") String collection,
            @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration)
            throws Exception {

        String url = "%s/collections/%s";
        Map<String, Object> config = getVectorDbInfo(hostOrKey, collection, configuration, url);
        config.putIfAbsent(METHOD_KEY, "DELETE");

        RestAPIConfig restAPIConfig = new RestAPIConfig(config);
        return executeRequest(restAPIConfig).map(v -> (Map<String, Object>) v).map(MapResult::new);
    }

    @Procedure("apoc.vectordb.qdrant.upsert")
    @Description(
            "apoc.vectordb.qdrant.upsert(hostOrKey, collection, vectors, $configuration) - Upserts, in the collection with the name specified in the 2nd parameter, the vectors [{id: 'id', vector: '<vectorDb>', medatada: '<metadata>'}]")
    public Stream<MapResult> upsert(
            @Name("hostOrKey") String hostOrKey,
            @Name("collection") String collection,
            @Name("vectors") List<Map<String, Object>> vectors,
            @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration)
            throws Exception {

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
                .collect(Collectors.toList());
        Map<String, Object> additionalBodies = Map.of("points", point);
        RestAPIConfig restAPIConfig = new RestAPIConfig(config, Map.of(), additionalBodies);
        return executeRequest(restAPIConfig).map(v -> (Map<String, Object>) v).map(MapResult::new);
    }

    @Procedure("apoc.vectordb.qdrant.delete")
    @Description(
            "apoc.vectordb.qdrant.delete(hostOrKey, collection, ids, $configuration) - Deletes the vectors with the specified `ids`")
    public Stream<MapResult> delete(
            @Name("hostOrKey") String hostOrKey,
            @Name("collection") String collection,
            @Name("vectors") List<Object> ids,
            @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration)
            throws Exception {

        String url = "%s/collections/%s/points/delete";
        Map<String, Object> config = getVectorDbInfo(hostOrKey, collection, configuration, url);
        config.putIfAbsent(METHOD_KEY, "POST");

        Map<String, Object> additionalBodies = Map.of("points", ids);
        RestAPIConfig apiConfig = new RestAPIConfig(config, Map.of(), additionalBodies);
        return executeRequest(apiConfig).map(v -> (Map<String, Object>) v).map(MapResult::new);
    }

    @Procedure(value = "apoc.vectordb.qdrant.get")
    @Description(
            "apoc.vectordb.qdrant.get(hostOrKey, collection, ids, $configuration) - Gets the vectors with the specified `ids`")
    public Stream<EmbeddingResult> get(
            @Name("hostOrKey") String hostOrKey,
            @Name("collection") String collection,
            @Name("ids") List<Object> ids,
            @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration)
            throws Exception {
        setReadOnlyMappingMode(configuration);
        return getCommon(hostOrKey, collection, ids, configuration);
    }

    @Procedure(value = "apoc.vectordb.qdrant.getAndUpdate", mode = Mode.WRITE)
    @Description(
            "apoc.vectordb.qdrant.getAndUpdate(hostOrKey, collection, ids, $configuration) - Gets the vectors with the specified `ids`, and optionally creates/updates neo4j entities")
    public Stream<EmbeddingResult> getAndUpdate(
            @Name("hostOrKey") String hostOrKey,
            @Name("collection") String collection,
            @Name("ids") List<Object> ids,
            @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration)
            throws Exception {
        return getCommon(hostOrKey, collection, ids, configuration);
    }

    private Stream<EmbeddingResult> getCommon(
            String hostOrKey, String collection, List<Object> ids, Map<String, Object> configuration) throws Exception {
        String url = "%s/collections/%s/points";
        Map<String, Object> config = getVectorDbInfo(hostOrKey, collection, configuration, url);

        VectorEmbeddingConfig conf = DB_HANDLER.getEmbedding().fromGet(config, procedureCallContext, ids, collection);

        return getEmbeddingResultStream(conf, procedureCallContext, tx);
    }

    @Procedure(value = "apoc.vectordb.qdrant.query")
    @Description(
            "apoc.vectordb.qdrant.query(hostOrKey, collection, vector, filter, limit, $configuration) - Retrieves closest vectors from the defined `vector`, `limit` of results, in the collection with the name specified in the 2nd parameter")
    public Stream<EmbeddingResult> query(
            @Name("hostOrKey") String hostOrKey,
            @Name("collection") String collection,
            @Name(value = "vector", defaultValue = "[]") List<Double> vector,
            @Name(value = "filter", defaultValue = "{}") Map<String, Object> filter,
            @Name(value = "limit", defaultValue = "10") long limit,
            @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration)
            throws Exception {
        setReadOnlyMappingMode(configuration);
        return queryCommon(hostOrKey, collection, vector, filter, limit, configuration);
    }

    @Procedure(value = "apoc.vectordb.qdrant.queryAndUpdate", mode = Mode.WRITE)
    @Description(
            "apoc.vectordb.chroma.queryAndUpdate(hostOrKey, collection, vector, filter, limit, $configuration) - Retrieves closest vectors from the defined `vector`, `limit` of results, in the collection with the name specified in the 2nd parameter, and optionally creates/updates neo4j entities")
    public Stream<EmbeddingResult> queryAndUpdate(
            @Name("hostOrKey") String hostOrKey,
            @Name("collection") String collection,
            @Name(value = "vector", defaultValue = "[]") List<Double> vector,
            @Name(value = "filter", defaultValue = "{}") Map<String, Object> filter,
            @Name(value = "limit", defaultValue = "10") long limit,
            @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration)
            throws Exception {
        return queryCommon(hostOrKey, collection, vector, filter, limit, configuration);
    }

    private Stream<EmbeddingResult> queryCommon(
            String hostOrKey,
            String collection,
            List<Double> vector,
            Map<String, Object> filter,
            long limit,
            Map<String, Object> configuration)
            throws Exception {
        String url = "%s/collections/%s/points/search";
        Map<String, Object> config = getVectorDbInfo(hostOrKey, collection, configuration, url);

        VectorEmbeddingConfig conf =
                DB_HANDLER.getEmbedding().fromQuery(config, procedureCallContext, vector, filter, limit, collection);

        return getEmbeddingResultStream(conf, procedureCallContext, tx);
    }

    private Map<String, Object> getVectorDbInfo(
            String hostOrKey, String collection, Map<String, Object> configuration, String templateUrl) {
        return getCommonVectorDbInfo(hostOrKey, collection, configuration, templateUrl, DB_HANDLER);
    }
}
