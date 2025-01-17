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

import static apoc.ml.RestAPIConfig.METHOD_KEY;
import static apoc.vectordb.VectorDb.executeRequest;
import static apoc.vectordb.VectorDb.getEmbeddingResultStream;
import static apoc.vectordb.VectorDbHandler.Type.PINECONE;
import static apoc.vectordb.VectorDbUtil.getCommonVectorDbInfo;

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
public class Pinecone {
    public static final VectorDbHandler DB_HANDLER = PINECONE.get();

    @Context
    public ProcedureCallContext procedureCallContext;

    @Context
    public Transaction tx;

    @Context
    public GraphDatabaseService db;

    @Procedure("apoc.vectordb.pinecone.info")
    @Description(
            "apoc.vectordb.pinecone.info(hostOrKey, index, $configuration) - Get information about the specified existing index or throws an error if it does not exist")
    public Stream<MapResult> getInfo(
            @Name("hostOrKey") String hostOrKey,
            @Name("index") String index,
            @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration)
            throws Exception {
        String url = "%s/indexes/%s";
        Map<String, Object> config = getVectorDbInfo(hostOrKey, index, configuration, url);

        RestAPIConfig restAPIConfig = new RestAPIConfig(config, Map.of(), Map.of());
        return executeRequest(restAPIConfig).map(v -> (Map<String, Object>) v).map(MapResult::new);
    }

    @Procedure("apoc.vectordb.pinecone.createCollection")
    @Description(
            "apoc.vectordb.pinecone.createCollection(hostOrKey, index, similarity, size, $configuration) - Creates a index, with the name specified in the 2nd parameter, and with the specified `similarity` and `size`")
    public Stream<MapResult> createCollection(
            @Name("hostOrKey") String hostOrKey,
            @Name("index") String index,
            @Name("similarity") String similarity,
            @Name("size") Long size,
            @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration)
            throws Exception {
        String url = "%s/indexes";
        Map<String, Object> config = getVectorDbInfo(hostOrKey, index, configuration, url);
        config.putIfAbsent(METHOD_KEY, "POST");

        Map<String, Object> additionalBodies = Map.of(
                "name", index,
                "dimension", size,
                "metric", similarity);
        RestAPIConfig restAPIConfig = new RestAPIConfig(config, Map.of(), additionalBodies);
        return executeRequest(restAPIConfig).map(v -> (Map<String, Object>) v).map(MapResult::new);
    }

    @Procedure("apoc.vectordb.pinecone.deleteCollection")
    @Description(
            "apoc.vectordb.pinecone.deleteCollection(hostOrKey, index, $configuration) - Deletes a index with the name specified in the 2nd parameter")
    public Stream<MapResult> deleteCollection(
            @Name("hostOrKey") String hostOrKey,
            @Name("index") String index,
            @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration)
            throws Exception {

        String url = "%s/indexes/%s";
        Map<String, Object> config = getVectorDbInfo(hostOrKey, index, configuration, url);
        config.putIfAbsent(METHOD_KEY, "DELETE");

        RestAPIConfig restAPIConfig = new RestAPIConfig(config);
        return executeRequest(restAPIConfig).map(v -> (Map<String, Object>) v).map(MapResult::new);
    }

    @Procedure("apoc.vectordb.pinecone.upsert")
    @Description(
            "apoc.vectordb.pinecone.upsert(hostOrKey, index, vectors, $configuration) - Upserts, in the index with the name specified in the 2nd parameter, the vectors [{id: 'id', vector: '<vectorDb>', medatada: '<metadata>'}]")
    public Stream<MapResult> upsert(
            @Name("hostOrKey") String hostOrKey,
            @Name("index") String index,
            @Name("vectors") List<Map<String, Object>> vectors,
            @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration)
            throws Exception {

        String url = "%s/vectors/upsert";

        Map<String, Object> config = getVectorDbInfo(hostOrKey, index, configuration, url);
        config.putIfAbsent(METHOD_KEY, "POST");

        vectors = vectors.stream()
                .map(i -> {
                    Map<String, Object> map = new HashMap<>(i);
                    map.putIfAbsent("values", map.remove("vector"));
                    return map;
                })
                .collect(Collectors.toList());

        Map<String, Object> additionalBodies = Map.of("vectors", vectors);
        RestAPIConfig restAPIConfig = new RestAPIConfig(config, Map.of(), additionalBodies);
        return executeRequest(restAPIConfig).map(v -> (Map<String, Object>) v).map(MapResult::new);
    }

    @Procedure("apoc.vectordb.pinecone.delete")
    @Description(
            "apoc.vectordb.pinecone.delete(hostOrKey, index, ids, $configuration) - Delete the vectors with the specified `ids`")
    public Stream<MapResult> delete(
            @Name("hostOrKey") String hostOrKey,
            @Name("index") String index,
            @Name("vectors") List<Object> ids,
            @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration)
            throws Exception {

        String url = "%s/vectors/delete";
        Map<String, Object> config = getVectorDbInfo(hostOrKey, index, configuration, url);
        config.putIfAbsent(METHOD_KEY, "POST");

        Map<String, Object> additionalBodies = Map.of("ids", ids);
        RestAPIConfig apiConfig = new RestAPIConfig(config, Map.of(), additionalBodies);
        return executeRequest(apiConfig).map(v -> (Map<String, Object>) v).map(MapResult::new);
    }

    @Procedure(value = "apoc.vectordb.pinecone.get")
    @Description(
            "apoc.vectordb.pinecone.get(hostOrKey, index, ids, $configuration) - Get the vectors with the specified `ids`")
    public Stream<VectorDbUtil.EmbeddingResult> get(
            @Name("hostOrKey") String hostOrKey,
            @Name("index") String index,
            @Name("ids") List<Object> ids,
            @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration)
            throws Exception {
        return getCommon(hostOrKey, index, ids, configuration);
    }

    @Procedure(value = "apoc.vectordb.pinecone.getAndUpdate", mode = Mode.WRITE)
    @Description(
            "apoc.vectordb.pinecone.getAndUpdate(hostOrKey, index, ids, $configuration) - Get the vectors with the specified `ids`")
    public Stream<VectorDbUtil.EmbeddingResult> getAndUpdate(
            @Name("hostOrKey") String hostOrKey,
            @Name("index") String index,
            @Name("ids") List<Object> ids,
            @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration)
            throws Exception {
        return getCommon(hostOrKey, index, ids, configuration);
    }

    private Stream<VectorDbUtil.EmbeddingResult> getCommon(
            String hostOrKey, String index, List<Object> ids, Map<String, Object> configuration) throws Exception {
        String url = "%s/vectors/fetch";
        Map<String, Object> config = getVectorDbInfo(hostOrKey, index, configuration, url);

        VectorEmbeddingConfig conf = DB_HANDLER.getEmbedding().fromGet(config, procedureCallContext, ids, index);

        return getEmbeddingResultStream(conf, procedureCallContext, tx, v -> {
            Object vectors = ((Map) v).get("vectors");
            return ((Map) vectors).values().stream();
        });
    }

    @Procedure(value = "apoc.vectordb.pinecone.query")
    @Description(
            "apoc.vectordb.pinecone.query(hostOrKey, index, vector, filter, limit, $configuration) - Retrieve closest vectors the the defined `vector`, `limit` of results,  in the index with the name specified in the 2nd parameter")
    public Stream<VectorDbUtil.EmbeddingResult> query(
            @Name("hostOrKey") String hostOrKey,
            @Name("index") String index,
            @Name(value = "vector", defaultValue = "[]") List<Double> vector,
            @Name(value = "filter", defaultValue = "{}") Map<String, Object> filter,
            @Name(value = "limit", defaultValue = "10") long limit,
            @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration)
            throws Exception {
        return queryCommon(hostOrKey, index, vector, filter, limit, configuration);
    }

    @Procedure(value = "apoc.vectordb.pinecone.queryAndUpdate", mode = Mode.WRITE)
    @Description(
            "apoc.vectordb.pinecone.queryAndUpdate(hostOrKey, index, vector, filter, limit, $configuration) - Retrieve closest vectors the the defined `vector`, `limit` of results,  in the index with the name specified in the 2nd parameter")
    public Stream<VectorDbUtil.EmbeddingResult> queryAndUpdate(
            @Name("hostOrKey") String hostOrKey,
            @Name("index") String index,
            @Name(value = "vector", defaultValue = "[]") List<Double> vector,
            @Name(value = "filter", defaultValue = "{}") Map<String, Object> filter,
            @Name(value = "limit", defaultValue = "10") long limit,
            @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration)
            throws Exception {
        return queryCommon(hostOrKey, index, vector, filter, limit, configuration);
    }

    private Stream<VectorDbUtil.EmbeddingResult> queryCommon(
            String hostOrKey,
            String index,
            List<Double> vector,
            Map<String, Object> filter,
            long limit,
            Map<String, Object> configuration)
            throws Exception {
        String url = "%s/query";
        Map<String, Object> config = getVectorDbInfo(hostOrKey, index, configuration, url);

        VectorEmbeddingConfig conf =
                DB_HANDLER.getEmbedding().fromQuery(config, procedureCallContext, vector, filter, limit, index);

        return getEmbeddingResultStream(conf, procedureCallContext, tx, v -> {
            Map map = (Map) v;
            return ((List) map.get("matches")).stream();
        });
    }

    private Map<String, Object> getVectorDbInfo(
            String hostOrKey, String index, Map<String, Object> configuration, String templateUrl) {
        return getCommonVectorDbInfo(hostOrKey, index, configuration, templateUrl, DB_HANDLER);
    }
}
