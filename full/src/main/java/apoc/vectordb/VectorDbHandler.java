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


import java.util.HashMap;
import java.util.Map;

import static apoc.ml.RestAPIConfig.HEADERS_KEY;

public interface VectorDbHandler {
    default Map<String, Object> getCredentials(Object credentialsObj, Map<String, Object> config) {
        Map headers = (Map) config.getOrDefault(HEADERS_KEY, new HashMap<>());
        headers.putIfAbsent("Authorization", "Bearer " + credentialsObj);
        config.put(HEADERS_KEY, headers);
        return config;
    }

    String getUrl(String hostOrKey);
    VectorEmbeddingHandler getEmbedding();
    String getLabel();

    enum Type {
        CHROMA(new ChromaHandler()),
        QDRANT(new QdrantHandler()),
        PINECONE(new PineconeHandler()),
        MILVUS(new MilvusHandler()),
        WEAVIATE(new WeaviateHandler());

        private final VectorDbHandler handler;

        Type(VectorDbHandler handler) {
            this.handler = handler;
        }

        public VectorDbHandler get() {
            return handler;
        }
    }
}
