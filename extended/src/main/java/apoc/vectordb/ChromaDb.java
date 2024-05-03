package apoc.vectordb;

import apoc.Extended;
import apoc.ml.RestAPIConfig;
import apoc.result.ListResult;
import apoc.result.MapResult;
import apoc.util.UrlResolver;
import org.apache.commons.collections4.CollectionUtils;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static apoc.ml.RestAPIConfig.*;
import static apoc.util.ExtendedUtil.listOfMapToMapOfLists;
import static apoc.util.MapUtil.map;
import static apoc.vectordb.VectorDb.executeRequest;
import static apoc.vectordb.VectorDb.getEmbeddingResultStream;
import static apoc.vectordb.VectorDbUtil.*;
import static apoc.vectordb.VectorEmbedding.Type.CHROMA;
import static apoc.vectordb.VectorEmbeddingConfig.*;

@Extended
public class ChromaDb {

    @Context
    public ProcedureCallContext procedureCallContext;

    @Context
    public Transaction tx;

    @Context
    public GraphDatabaseService db;

    @Context
    public URLAccessChecker urlAccessChecker;

    @Procedure("apoc.vectordb.chroma.createCollection")
    @Description("apoc.vectordb.chroma.createCollection(hostOrKey, collection, similarity, size, $config)")
    public Stream<MapResult> createCollection(@Name("hostOrKey") String hostOrKey,
                                    @Name("collection") String collection,
                                    @Name("similarity") String similarity,
                                    @Name("size") Long size,
                                    @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) throws Exception {
        var config = new HashMap<>(configuration);

        String qdrantUrl = getChromaUrl(hostOrKey);
        String endpoint = "%s/api/v1/collections".formatted(qdrantUrl);
        getEndpoint(config, endpoint);
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
    @Description("apoc.vectordb.chroma.deleteCollection")
    public Stream<MapResult> deleteCollection(
            @Name("hostOrKey") String hostOrKey,
            @Name("collection") String collection, 
            @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) throws Exception {
        var config = new HashMap<>(configuration);

        String qdrantUrl = getChromaUrl(hostOrKey);
        String endpoint = "%s/api/v1/collections/%s".formatted(qdrantUrl, collection);
        getEndpoint(config, endpoint);
        config.putIfAbsent(METHOD_KEY, "DELETE");

        RestAPIConfig restAPIConfig = new RestAPIConfig(config, Map.of(), Map.of());
        return executeRequest(restAPIConfig, urlAccessChecker)
                .map(v -> (Map<String,Object>)v)
                .map(MapResult::new);
    }

    @Procedure("apoc.vectordb.chroma.upsert")
    @Description("apoc.vectordb.chroma.upsert")
    public Stream<MapResult> upsert(
            @Name("hostOrKey") String hostOrKey,
            @Name("collection") String collection,
            @Name("vectors") List<Map<String, Object>> vectors,
            @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) throws Exception {
        var config = new HashMap<>(configuration);

        String qdrantUrl = getChromaUrl(hostOrKey);
        String endpoint = "%s/api/v1/collections/%s/upsert".formatted(qdrantUrl, collection);
        getEndpoint(config, endpoint);
        
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
    
    @Procedure(value = "apoc.vectordb.chroma.delete", mode = Mode.SCHEMA)
    @Description("apoc.vectordb.chroma.delete()")
    public Stream<ListResult> delete(@Name("hostOrKey") String hostOrKey,
                                     @Name("collection") String collection,
                                     @Name("ids") List<Object> ids,
                                     @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) throws Exception {
        var config = new HashMap<>(configuration);

        String qdrantUrl = getChromaUrl(hostOrKey);
        String endpoint = "%s/api/v1/collections/%s/delete".formatted(qdrantUrl, collection);
        getEndpoint(config, endpoint);

        VectorEmbeddingConfig apiConfig = CHROMA.get().fromGet(config, procedureCallContext, getStringIds(ids));
        return executeRequest(apiConfig.getApiConfig(), urlAccessChecker)
                .map(v -> (List) v)
                .map(ListResult::new);
    }

    @Procedure(value = "apoc.vectordb.chroma.get", mode = Mode.SCHEMA)
    @Description("apoc.vectordb.chroma.get()")
    public Stream<EmbeddingResult> query(@Name("hostOrKey") String hostOrKey,
                                                      @Name("collection") String collection,
                                                      @Name("ids") List<Object> ids,
                                                      @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) throws Exception {
        var config = new HashMap<>(configuration);

        String qdrantUrl = getChromaUrl(hostOrKey);
        String endpoint = "%s/api/v1/collections/%s/get".formatted(qdrantUrl, collection);
        getEndpoint(config, endpoint);

        VectorEmbeddingConfig apiConfig = CHROMA.get().fromGet(config, procedureCallContext, ids);
        return getEmbeddingResultStream(apiConfig, procedureCallContext, urlAccessChecker, db, tx,
                v -> listToMap((Map) v).stream());
    }

    @Procedure(value = "apoc.vectordb.chroma.query", mode = Mode.SCHEMA)
    @Description("apoc.vectordb.chroma.query()")
    public Stream<EmbeddingResult> query(@Name("hostOrKey") String hostOrKey,
                                                      @Name("collection") String collection,
                                                      @Name(value = "vector", defaultValue = "[]") List<Double> vector,
                                                      @Name(value = "filter", defaultValue = "{}") Map<String, Object> filter,
                                                      @Name(value = "limit", defaultValue = "10") long limit,
                                                      @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) throws Exception {

        var config = new HashMap<>(configuration);

        String qdrantUrl = getChromaUrl(hostOrKey);
        String endpoint = "%s/api/v1/collections/%s/query".formatted(qdrantUrl, collection);
        getEndpoint(config, endpoint);

        VectorEmbeddingConfig apiConfig = CHROMA.get().fromQuery(config, procedureCallContext, vector, filter, limit, collection);
        return getEmbeddingResultStream(apiConfig, procedureCallContext, urlAccessChecker, db, tx,
                v -> listOfListsToMap((Map) v).stream());
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

    protected String getChromaUrl(String hostOrKey) {
        return new UrlResolver("http", "localhost", 8000).getUrl("chroma", hostOrKey);
    }
}
