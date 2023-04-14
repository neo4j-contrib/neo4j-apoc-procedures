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
package apoc.couchbase.document;

import com.couchbase.client.core.msg.kv.MutationToken;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.GetResult;
import com.couchbase.client.java.kv.MutationResult;

import java.util.Map;

/**
 * Represents a {@link MutationResult} or a {@link GetResult} (in case of apoc.couchbase.get procedure)
 * that contains a <code>JSON object</code> as the
 * content.
 * <p/>
 * The <code>JSON object</code> here comes in the form of a <code>Map</code>.
 *
 * @see JsonObject
 */
public class CouchbaseJsonDocument extends CouchbaseObjectDocument<Map<String, Object>> {

    /**
     * The json content of the {@link GetResult}.
     */
    public Map<String, Object> content;

    public CouchbaseJsonDocument(GetResult getResult, String id) {
        this(getResult, id, null);
    }

    public CouchbaseJsonDocument(GetResult getResult, String id, MutationToken mutationToken) {
        super(getResult, id, mutationToken);
        this.content = getResult.contentAsObject().toMap();
    }

    @Override
    public Map<String, Object> getContent() {
        return this.content;
    }

    @Override
    public String toString() {
        return "CouchbaseJsonDocument {" +
                "content=" + content +
                ", id='" + id + '\'' +
                ", expiry=" + expiry +
                ", cas=" + cas +
                ", mutationToken=" + mutationToken +
                '}';
    }
}
