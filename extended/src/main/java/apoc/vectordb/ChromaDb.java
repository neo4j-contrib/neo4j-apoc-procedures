package apoc.vectordb;

import apoc.Extended;
import apoc.ml.RestAPIConfig;
import apoc.result.ListResult;
import apoc.result.MapResult;
import apoc.util.CollectionUtils;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.security.URLAccessChecker;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static apoc.ml.RestAPIConfig.*;
import static apoc.util.ExtendedUtil.listOfMapToMapOfLists;
import static apoc.util.MapUtil.map;
import static apoc.vectordb.VectorDb.executeRequest;
import static apoc.vectordb.VectorDb.getEmbeddingResultStream;
import static apoc.vectordb.VectorDbHandler.Type.CHROMA;
import static apoc.vectordb.VectorDbUtil.*;
import static apoc.vectordb.VectorEmbeddingConfig.*;

@Extended
public class ChromaDb {
    public static final VectorDbHandler DB_HANDLER = CHROMA.get();

    @Context
    public ProcedureCallContext procedureCallContext;

    @Context
    public Transaction tx;

    @Context
    public GraphDatabaseService db;

    @Context
    public URLAccessChecker urlAccessChecker;

    @Procedure("apoc.vectordb.chroma.createCollection")
    @Description("apoc.vectordb.chroma.createCollection(hostOrKey, collection, similarity, size, $configuration) - Creates a collection, with the name specified in the 2nd parameter, and with the specified `similarity` and `size`")
    public Stream<MapResult> createCollection(@Name("hostOrKey") String hostOrKey,
                                    @Name("collection") String collection,
                                    @Name("similarity") String similarity,
                                    @Name("size") Long size,
                                    @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) throws Exception {
        String url = "%s/api/v1/collections";
        Map<String, Object> config = getVectorDbInfo(hostOrKey, collection, configuration, url);
        config.putIfAbsent(METHOD_KEY, "POST");

        Map<String, Object> metadata = Map.of("hnsw:space", similarity,
                "size", size);
        Map<String, Object> additionalBodies = Map.of("name", collection, "metadata", metadata);
        RestAPIConfig restAPIConfig = new RestAPIConfig(config, Map.of(), additionalBodies);
        return executeRequest(restAPIConfig, urlAccessChecker)
                .map(v -> (Map<String,Object>)v)
                .map(MapResult::new);
    }

    @Procedure("apoc.vectordb.chroma.deleteCollection")
    @Description("apoc.vectordb.chroma.deleteCollection(hostOrKey, collection, $configuration) - Deletes a collection with the name specified in the 2nd parameter")
    public Stream<MapResult> deleteCollection(
            @Name("hostOrKey") String hostOrKey,
            @Name("collection") String collection, 
            @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) throws Exception {
        String url = "%s/api/v1/collections/%s";
        Map<String, Object> config = getVectorDbInfo(hostOrKey, collection, configuration, url);
        config.putIfAbsent(METHOD_KEY, "DELETE");

        RestAPIConfig restAPIConfig = new RestAPIConfig(config, Map.of(), Map.of());
        return executeRequest(restAPIConfig, urlAccessChecker)
                .map(v -> (Map<String,Object>)v)
                .map(MapResult::new);
    }

    @Procedure("apoc.vectordb.chroma.upsert")
    @Description("apoc.vectordb.chroma.upsert(hostOrKey, collection, vectors, $configuration) - Upserts, in the collection with the name specified in the 2nd parameter, the vectors [{id: 'id', vector: '<vectorDb>', medatada: '<metadata>'}]")
    public Stream<MapResult> upsert(
            @Name("hostOrKey") String hostOrKey,
            @Name("collection") String collection,
            @Name("vectors") List<Map<String, Object>> vectors,
            @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) throws Exception {
        String url = "%s/api/v1/collections/%s/upsert";
        Map<String, Object> config = getVectorDbInfo(hostOrKey, collection, configuration, url);
        
        Map<String, String> mapKeys = Map.of("id", "ids",
                "vector", "embeddings",
                "metadata", "metadatas",
                "text", "documents");

        // transform to format digestible by RestAPI,
        // that is from [{id: <id1>, vector: <vector1>,,,}, {id: <id2>, vector: <vector2>,,,}] 
        // to {ids: [<id1>, <id2>], vectors: [<vector1>, <vector2>]}
        Map<Object, List> additionalBodies = listOfMapToMapOfLists(mapKeys, vectors);
        additionalBodies.compute( "ids", (k,v) -> getStringIds(v) );

        RestAPIConfig restAPIConfig = new RestAPIConfig(config, Map.of(), additionalBodies);
        return executeRequest(restAPIConfig, urlAccessChecker)
                .map(v -> (Map<String,Object>)v)
                .map(MapResult::new);
    }

    @Procedure(value = "apoc.vectordb.chroma.delete")
    @Description("apoc.vectordb.chroma.delete(hostOrKey, collection, ids, $configuration) - Deletes the vectors with the specified `ids`")
    public Stream<ListResult> delete(@Name("hostOrKey") String hostOrKey,
                                     @Name("collection") String collection,
                                     @Name("ids") List<Object> ids,
                                     @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) throws Exception {
        String url = "%s/api/v1/collections/%s/delete";
        Map<String, Object> config = getVectorDbInfo(hostOrKey, collection, configuration, url);

        VectorEmbeddingConfig conf = DB_HANDLER.getEmbedding().fromGet(config, procedureCallContext, getStringIds(ids), collection);
        return executeRequest(conf.getApiConfig(), urlAccessChecker)
                .map(v -> (List) v)
                .map(ListResult::new);
    }

    @Procedure(value = "apoc.vectordb.chroma.get")
    @Description("apoc.vectordb.chroma.get(hostOrKey, collection, ids, $configuration) - Gets the vectors with the specified `ids`")
    public Stream<EmbeddingResult> get(@Name("hostOrKey") String hostOrKey,
                                                      @Name("collection") String collection,
                                                      @Name("ids") List<Object> ids,
                                                      @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) throws Exception {
        setReadOnlyMappingMode(configuration);
        return getCommon(hostOrKey, collection, ids, configuration);
    }

    @Procedure(value = "apoc.vectordb.chroma.getAndUpdate", mode = Mode.WRITE)
    @Description("apoc.vectordb.chroma.getAndUpdate(hostOrKey, collection, ids, $configuration) - Gets the vectors with the specified `ids`, and optionally creates/updates neo4j entities")
    public Stream<EmbeddingResult> getAndUpdate(@Name("hostOrKey") String hostOrKey,
                                                      @Name("collection") String collection,
                                                      @Name("ids") List<Object> ids,
                                                      @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) throws Exception {
        return getCommon(hostOrKey, collection, ids, configuration);
    }

    private Stream<EmbeddingResult> getCommon(String hostOrKey, String collection, List<Object> ids, Map<String, Object> configuration) throws Exception {
        String url = "%s/api/v1/collections/%s/get";
        Map<String, Object> config = getVectorDbInfo(hostOrKey, collection, configuration, url);

        VectorEmbeddingConfig apiConfig = DB_HANDLER.getEmbedding().fromGet(config, procedureCallContext, ids, collection);
        
        return getEmbeddingResultStream(apiConfig, procedureCallContext, urlAccessChecker, tx,
                v -> listToMap((Map) v).stream());
    }

    @Procedure(value = "apoc.vectordb.chroma.query")
    @Description("apoc.vectordb.chroma.query(hostOrKey, collection, vector, filter, limit, $configuration) - Retrieves closest vectors from the defined `vector`, `limit` of results, in the collection with the name specified in the 2nd parameter")
    public Stream<EmbeddingResult> query(@Name("hostOrKey") String hostOrKey,
                                         @Name("collection") String collection,
                                         @Name(value = "vector", defaultValue = "[]") List<Double> vector,
                                         @Name(value = "filter", defaultValue = "{}") Map<String, Object> filter,
                                         @Name(value = "limit", defaultValue = "10") long limit,
                                         @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) throws Exception {
        setReadOnlyMappingMode(configuration);
        return queryCommon(hostOrKey, collection, vector, filter, limit, configuration);
    }

    @Procedure(value = "apoc.vectordb.chroma.queryAndUpdate", mode = Mode.WRITE)
    @Description("apoc.vectordb.chroma.queryAndUpdate(hostOrKey, collection, vector, filter, limit, $configuration) - Retrieves closest vectors from the defined `vector`, `limit` of results, in the collection with the name specified in the 2nd parameter, and optionally creates/updates neo4j entities")
    public Stream<EmbeddingResult> queryAndUpdate(@Name("hostOrKey") String hostOrKey,
                                                      @Name("collection") String collection,
                                                      @Name(value = "vector", defaultValue = "[]") List<Double> vector,
                                                      @Name(value = "filter", defaultValue = "{}") Map<String, Object> filter,
                                                      @Name(value = "limit", defaultValue = "10") long limit,
                                                      @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) throws Exception {
        return queryCommon(hostOrKey, collection, vector, filter, limit, configuration);
    }

    private Stream<EmbeddingResult> queryCommon(String hostOrKey, String collection, List<Double> vector, Map<String, Object> filter, long limit, Map<String, Object> configuration) throws Exception {
        String url = "%s/api/v1/collections/%s/query";
        Map<String, Object> config = getVectorDbInfo(hostOrKey, collection, configuration, url);

        VectorEmbeddingConfig conf = DB_HANDLER.getEmbedding().fromQuery(config, procedureCallContext, vector, filter, limit, collection);
        return getEmbeddingResultStream(conf, procedureCallContext, urlAccessChecker, tx,
                v -> listOfListsToMap((Map) v).stream());
    }

    private Map<String, Object> getVectorDbInfo(String hostOrKey, String collection, Map<String, Object> configuration, String templateUrl) {
        return getCommonVectorDbInfo(hostOrKey, collection, configuration, templateUrl, DB_HANDLER);
    }

    private static List<Map> listOfListsToMap(Map startMap) {
        List distances = startMap.get("distances") == null 
                ? null 
                : ((List<List>) startMap.get("distances"))
                        .get(0);
        List metadatas = startMap.get("metadatas") == null
                ? null
                : ((List<List>) startMap.get("metadatas"))
                .get(0);
        List documents = startMap.get("documents") == null
                ? null
                : ((List<List>) startMap.get("documents"))
                .get(0);
        List embeddings = startMap.get("embeddings") == null
                ? null
                : ((List<List>) startMap.get("embeddings"))
                .get(0);

        List ids = ((List<List>) startMap.get("ids")).get(0);

        return getMaps(distances, metadatas, documents, embeddings, ids);
    }

    private static List<Map> listToMap(Map startMap) {
        List distances = (List) startMap.get("distances");
        List metadatas = (List) startMap.get("metadatas");
        List documents = (List) startMap.get("documents");
        List embeddings = (List) startMap.get("embeddings");

        List ids = (List) startMap.get("ids");

        return getMaps(distances, metadatas, documents, embeddings, ids);
    }

    private static List<Map> getMaps(List distances, List metadatas, List documents, List embeddings, List ids) {
        final List<Map> result = new ArrayList<>();
        for (int i = 0; i < ids.size(); i++) {
            Map<String, Object> map = map(DEFAULT_ID, ids.get(i));
            if (CollectionUtils.isNotEmpty(distances)) {
                map.put(DEFAULT_SCORE, distances.get(i));
            }
            if (CollectionUtils.isNotEmpty(metadatas)) {
                map.put(DEFAULT_METADATA, metadatas.get(i));
            }
            if (CollectionUtils.isNotEmpty(documents)) {
                map.put(DEFAULT_TEXT, documents.get(i));
            }
            if (CollectionUtils.isNotEmpty(embeddings)) {
                map.put(DEFAULT_VECTOR, embeddings.get(i));
            }
            result.add(map);
        }

        return result;
    }
    
    private List<String> getStringIds(List<Object> ids) {
        return ids.stream().map(Object::toString).toList();
    }

}
