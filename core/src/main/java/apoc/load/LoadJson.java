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
package apoc.load;

import apoc.result.MapResult;
import apoc.result.ObjectResult;
import apoc.util.CompressionAlgo;
import apoc.util.JsonUtil;
import apoc.util.Util;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static apoc.util.CompressionConfig.COMPRESSION;
import static apoc.util.Util.setKernelStatus;

public class LoadJson {

    private static final String AUTH_HEADER_KEY = "Authorization";
    private static final String LOAD_TYPE = "json";

    @Context
    public GraphDatabaseService db;

    @Context
    public Transaction tx;

    @SuppressWarnings("unchecked")
    @Procedure
    @Description("apoc.load.jsonArray('url') YIELD value - load array from JSON URL (e.g. web-api) to import JSON as stream of values")
    public Stream<ObjectResult> jsonArray(@Name("url") String url, @Name(value = "path",defaultValue = "") String path, @Name(value = "config",defaultValue = "{}") Map<String, Object> config) {
        AtomicInteger rows = new AtomicInteger();
        return JsonUtil.loadJson(url, null, null, path, true, (List<String>) config.get("pathOptions"))
                .flatMap((value) -> {
                    if (value instanceof List) {
                        setKernelStatus(tx, "rows", rows.incrementAndGet());
                        List list = (List) value;
                        if (list.isEmpty()) return Stream.empty();
                        if (list.get(0) instanceof Map) return list.stream().map(ObjectResult::new);
                    }
                    return Stream.of(new ObjectResult(value));
                });
        // throw new RuntimeException("Incompatible Type " + (value == null ? "null" : value.getClass()));
    }

    @Procedure
    @Description("apoc.load.json('urlOrKeyOrBinary',path, config) YIELD value - import JSON as stream of values if the JSON was an array or a single value if it was a map")
    public Stream<MapResult> json(@Name("urlOrKeyOrBinary") Object urlOrKeyOrBinary, @Name(value = "path",defaultValue = "") String path, @Name(value = "config",defaultValue = "{}") Map<String, Object> config) {
        return jsonParams(urlOrKeyOrBinary,null,null, path, config);
    }

    @SuppressWarnings("unchecked")
    @Procedure
    @Description("apoc.load.jsonParams('urlOrKeyOrBinary',{header:value},payload, config) YIELD value - load from JSON URL (e.g. web-api) while sending headers / payload to import JSON as stream of values if the JSON was an array or a single value if it was a map")
    public Stream<MapResult> jsonParams(@Name("urlOrKeyOrBinary") Object urlOrKeyOrBinary, @Name("headers") Map<String,Object> headers, @Name("payload") String payload, @Name(value = "path",defaultValue = "") String path, @Name(value = "config",defaultValue = "{}") Map<String, Object> config) {
        if (config == null) config = Collections.emptyMap();
        boolean failOnError = (boolean) config.getOrDefault("failOnError", true);
        String compressionAlgo = (String) config.getOrDefault(COMPRESSION, CompressionAlgo.NONE.name());
        List<String> pathOptions = (List<String>) config.get("pathOptions");
        return loadJsonStream(urlOrKeyOrBinary, headers, payload, path, failOnError, compressionAlgo, pathOptions, tx);
    }

    public static Stream<MapResult> loadJsonStream(@Name("url") Object url, @Name("headers") Map<String, Object> headers, @Name("payload") String payload) {
        return loadJsonStream(url, headers, payload, "", true, null, null, null);
    }
    public static Stream<MapResult> loadJsonStream(@Name("urlOrKeyOrBinary") Object urlOrKeyOrBinary, @Name("headers") Map<String, Object> headers, @Name("payload") String payload, String path, boolean failOnError, String compressionAlgo, List<String> pathOptions, Transaction tx) {
        if (urlOrKeyOrBinary instanceof String) {
            headers = null != headers ? headers : new HashMap<>();
            headers.putAll(Util.extractCredentialsIfNeeded((String) urlOrKeyOrBinary, failOnError));
        }
        Stream<Object> stream = JsonUtil.loadJson(urlOrKeyOrBinary,headers,payload, path, failOnError, compressionAlgo, pathOptions);
        AtomicInteger rows = new AtomicInteger();
        return stream.flatMap((value) -> {
            if (value instanceof Map) {
                setKernelStatus(tx, "rows", rows.incrementAndGet());
                return Stream.of(new MapResult((Map) value));
            }
            if (value instanceof List) {
                setKernelStatus(tx, "rows", rows.incrementAndGet());
                if (((List)value).isEmpty()) return Stream.empty();
                if (((List) value).get(0) instanceof Map)
                    return ((List) value).stream().map((v) -> new MapResult((Map) v));
                return Stream.of(new MapResult(Collections.singletonMap("result",value)));
            }
            if(!failOnError)
                throw new RuntimeException("Incompatible Type " + (value == null ? "null" : value.getClass()));
            else
                return Stream.of(new MapResult(Collections.emptyMap()));
        });
    }

}
