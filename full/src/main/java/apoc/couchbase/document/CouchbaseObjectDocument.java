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

import java.time.Instant;
import java.util.Map;

import com.couchbase.client.core.msg.kv.MutationToken;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.GetResult;
import com.couchbase.client.java.kv.MutationResult;

import static apoc.couchbase.document.CouchbaseUtils.convertMutationTokenToMap;

/**
 * Represents a {@link MutationResult} or a {@link GetResult}.
 *
 * @since 15.8.2016
 * @author inserpio
 * @see JsonObject
 */
public abstract class CouchbaseObjectDocument<T> implements CouchbaseDocument<T> {

  /**
   * The per-bucket unique ID of the {@link GetResult} or the {@link MutationResult}.
   */
  public String id;

  /**
   * The optional expiration time for the {@link GetResult} (0 if not set).
   */
  public long expiry;

  /**
   * The last-known CAS (<i>Compare And Swap</i>) value for the {@link MutationResult} (0 if not set).
   */
  public long cas;

  /**
   * The optional, opaque mutation token set after a successful mutation and if
   * enabled on the environment.
   */
  public Map<String, Object> mutationToken;


  public CouchbaseObjectDocument(GetResult getResult, String id, MutationToken mutationToken) {
    this.id = id;
    this.expiry = getResult.expiryTime().orElse(Instant.ofEpochMilli(0)).toEpochMilli();
    this.cas = getResult.cas();
    this.mutationToken = convertMutationTokenToMap(mutationToken);
  }

  @Override
  public String getId() {
    return this.id;
  }

  @Override
  public long getCas() {
    return this.cas;
  }

  @Override
  public long getExpiry() {
    return this.expiry;
  }

  @Override
  public Map<String, Object> getMutationToken() {
    return this.mutationToken;
  }

}
