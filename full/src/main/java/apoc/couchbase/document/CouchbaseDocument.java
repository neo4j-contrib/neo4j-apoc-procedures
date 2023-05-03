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

import java.util.Map;

import org.neo4j.procedure.Procedure;

import com.couchbase.client.java.AsyncBucket;

/**
 * This interface defines a Couchbase Document interface
 * with valid data types for neo4j procedures.
 * <p/>
 * it represents a Couchbase Server document which is stored in and
 * retrieved from a {@link AsyncBucket}.
 * 
 * @since 15.8.2016
 * @author inserpio
 * @see Procedure
 */
public interface CouchbaseDocument<T> {

	String getId();

	T getContent();

	long getCas();

	long getExpiry();

	Map<String, Object> getMutationToken();
}
