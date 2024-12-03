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

import apoc.result.LoadDataMapResultExtended;
import apoc.util.JsonUtilExtended;
import apoc.util.UtilExtended;
import org.neo4j.graphdb.security.URLAccessChecker;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.TerminationGuard;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class LoadJsonUtilsExtended {
    public static Stream<LoadDataMapResultExtended> loadJsonStream(
            @Name("urlOrKeyOrBinary") Object urlOrKeyOrBinary,
            @Name("headers") Map<String, Object> headers,
            @Name("payload") String payload,
            String path,
            boolean failOnError,
            String compressionAlgo,
            List<String> pathOptions,
            TerminationGuard terminationGuard,
            URLAccessChecker urlAccessChecker) {
        if (urlOrKeyOrBinary instanceof String) {
            headers = null != headers ? headers : new HashMap<>();
            headers.putAll(UtilExtended.extractCredentialsIfNeeded((String) urlOrKeyOrBinary, failOnError));
        }
        Stream<Object> stream = JsonUtilExtended.loadJson(
                urlOrKeyOrBinary, headers, payload, path, failOnError, compressionAlgo, pathOptions, urlAccessChecker);
        return stream.flatMap((value) -> {
            if (terminationGuard != null) {
                terminationGuard.check();
            }
            if (value instanceof Map) {
                return Stream.of(new LoadDataMapResultExtended((Map) value));
            }
            if (value instanceof List) {
                if (((List) value).isEmpty()) return Stream.empty();
                if (((List) value).get(0) instanceof Map)
                    return ((List) value).stream().map((v) -> {
                        if (terminationGuard != null) {
                            terminationGuard.check();
                        }
                        return new LoadDataMapResultExtended((Map) v);
                    });
                return Stream.of(new LoadDataMapResultExtended(Collections.singletonMap("result", value)));
            }
            if (!failOnError)
                throw new RuntimeException("Incompatible Type " + (value == null ? "null" : value.getClass()));
            else return Stream.of(new LoadDataMapResultExtended(Collections.emptyMap()));
        });
    }
}
