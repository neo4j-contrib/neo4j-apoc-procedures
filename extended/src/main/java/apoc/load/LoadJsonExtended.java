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

import apoc.Extended;
import apoc.result.LoadDataMapResult;
import apoc.util.CompressionAlgo;
import org.neo4j.graphdb.security.URLAccessChecker;
import org.neo4j.kernel.api.QueryLanguage;
import org.neo4j.kernel.api.procedure.QueryLanguageScope;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.TerminationGuard;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static apoc.load.LoadJsonUtilsExtended.loadJsonStream;
import static apoc.util.CompressionConfig.COMPRESSION;

@Extended
public class LoadJsonExtended {

    @Context
    public TerminationGuard terminationGuard;

    @Context
    public URLAccessChecker urlAccessChecker;

    @SuppressWarnings("unchecked")
    @Procedure(name = "apoc.load.jsonParams")
    @QueryLanguageScope(scope = {QueryLanguage.CYPHER_25})
    @Description(
            "Loads parameters from a JSON URL (e.g. web-API) as a stream of values if the given JSON file is a `LIST<ANY>`.\n"
                    + "If the given JSON file is a `MAP`, this procedure imports a single value instead.")
    public Stream<LoadDataMapResult> jsonParams(
            @Name(
                            value = "urlOrKeyOrBinary",
                            description = "The name of the file or binary data to import the data from.")
                    Object urlOrKeyOrBinary,
            @Name(value = "headers", description = "Headers to be used when connecting to the given URL.")
                    Map<String, Object> headers,
            @Name(value = "payload", description = "The payload to send when connecting to the given URL.")
                    String payload,
            @Name(
                            value = "path",
                            defaultValue = "",
                            description = "A JSON path expression used to extract a certain part from the list.")
                    String path,
            @Name(
                            value = "config",
                            defaultValue = "{}",
                            description =
                                    """
                    {
                        failOnError = true :: BOOLEAN,
                        pathOptions :: LIST<STRING>,
                        compression = ""NONE"" :: [""NONE"", ""BYTES"", ""GZIP"", ""BZIP2"", ""DEFLATE"", ""BLOCK_LZ4"", ""FRAMED_SNAPPY”]
                    }
                    """)
                    Map<String, Object> config) {
        if (config == null) config = Collections.emptyMap();
        boolean failOnError = (boolean) config.getOrDefault("failOnError", true);
        String compressionAlgo = (String) config.getOrDefault(COMPRESSION, CompressionAlgo.NONE.name());
        List<String> pathOptions = (List<String>) config.get("pathOptions");
        return loadJsonStream(
                urlOrKeyOrBinary,
                headers,
                payload,
                path,
                failOnError,
                compressionAlgo,
                pathOptions,
                terminationGuard,
                urlAccessChecker);
    }
}
