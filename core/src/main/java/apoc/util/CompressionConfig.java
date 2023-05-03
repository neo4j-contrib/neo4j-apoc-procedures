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
package apoc.util;

import java.nio.charset.Charset;
import java.util.Collections;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;

public class CompressionConfig {
    public static final String COMPRESSION = "compression";
    public static final String CHARSET = "charset";

    private final String compressionAlgo;
    private final Charset charset;

    public CompressionConfig(Map<String, Object> config) {
        this(config, CompressionAlgo.NONE.name());
    }

    public CompressionConfig(Map<String, Object> config, String defaultCompression) {
        if (config == null) config = Collections.emptyMap();
        this.compressionAlgo = (String) config.getOrDefault(COMPRESSION, defaultCompression);
        this.charset = Charset.forName((String) config.getOrDefault(CHARSET, UTF_8.name()));
    }

    public String getCompressionAlgo() {
        return compressionAlgo;
    }

    public Charset getCharset() {
        return charset;
    }
}
