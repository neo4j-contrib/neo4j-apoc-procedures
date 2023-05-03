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

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.query.QueryResult;

/**
 * Transports the list of {@link JsonObject}s retrieved by a N1QL query so that
 * it can be {@link Stream}-ed and returned by the procedures
 * <p/>
 * Every {@link JsonObject}s retrieved by a N1QL query is first converted into a
 * {@link Map Map&lt;String, Object&gt;} and then added to the embedded
 * {@link #queryResult} list.
 * 
 * @since 15.8.2016
 * @author inserpio
 * 
 * @see CouchbaseUtils#convertToCouchbaseQueryResult(List)
 * @see QueryResult
 */
public class CouchbaseQueryResult {

  public List<Map<String, Object>> queryResult;

  public CouchbaseQueryResult() {
    this.queryResult = null;
  }

  public CouchbaseQueryResult(List<Map<String, Object>> value) {
    this.queryResult = value;
  }
}
