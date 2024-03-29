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
package apoc.couchbase;

import apoc.Extended;
import apoc.couchbase.document.CouchbaseByteArrayDocument;
import apoc.couchbase.document.CouchbaseJsonDocument;
import apoc.couchbase.document.CouchbaseQueryResult;
import apoc.result.BooleanResult;
import apoc.util.MissingDependencyException;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

/**
 * Neo4j Procedures for <b>Couchbase integration</b>.
 * <p/>
 * All of the operations performed against the Couchbase Server are done through
 * a {@link CouchbaseConnection}.
 * <br/>
 * A {@link CouchbaseConnection} can be created via the {@link CouchbaseManager}
 * .{@link CouchbaseManager#getConnection getConnection} method.
 * <p/>
 * Available operations are:
 * <ul>
 * <li>{@link #get} to retrieve a json document by its unique ID</li>
 * <li>{@link #exists} to check whether a json document with the given ID does exist</li>
 * <li>{@link #insert} to insert a json document if it does not exist already</li>
 * <li>{@link #upsert} to insert or overwrite a json document</li>
 * <li>{@link #remove} to remove the json document identified by its unique ID</li>
 * <li>{@link #replace} to replace the content of the json document identified by its unique ID</li>
 * <li>{@link #append} to append a json document's content to an existing one</li>
 * <li>{@link #prepend} to prepend a json document's content to an existing one</li>
 * </ul>
 * <p/>
 * N1QL query can be executed via the following methods:
 * <ul>
 * <li>{@link #query} for plain un-parameterized N1QL statements</li>
 * <li>{@link #posParamsQuery} for N1QL statements with positional parameters</li>
 * <li>{@link #namedParamsQuery} for N1QL statements with named parameters</li>
 * </ul>
 * For instance, after inserting a JSON document this way:
 * <p/>
 * <code>
 * call apoc.couchbase.insert(['localhost'], 'default', 'artist:vincent_van_gogh', '{"firstName":"Vincent","secondName":"Willem","lastName":"Van Gogh","notableWorks":["Starry Night","Sunflowers","Bedroom in Arles","Portrait of Dr Gachet","Sorrow"]}')
 * </code>
 * <p/>
 * you can read the just inserted document via:
 * <p/>
 * <code>
 * call apoc.couchbase.query(['localhost'], 'default', 'select * from default') yield queryResult<br/>
 * unwind queryResult as queryResultRow<br/>
 * call apoc.convert.toMap(queryResultRow) yield value as queryResultRowMap<br/>
 * with queryResultRowMap.default as content<br/>
 * return content.firstName, content.secondName, content.lastName, content.notableWorks
 * </code>
 * <p/>
 * Using JSON fields instead of the &quot;*&quot; notation makes things a bit easier:
 * <p/>
 * <code>
 * call apoc.couchbase.query(['localhost'], 'default', 'select firstName, secondName, lastName, notableWorks from default') yield queryResult<br/>
 * unwind queryResult as queryRow<br/>
 * return queryRow.firstName, queryRow.secondName, queryRow.lastName, queryRow.notableWorks
 * </code>
 *
 * @author inserpio
 * @since 15.8.2016
 */
@Extended
public class Couchbase {

    public static final String COUCHBASE_MISSING_DEPS_ERROR =
            "Cannot find the jar into the plugins folder. \n" + "Please put these jar in the plugins folder : \n\n"
                    + "java-client-x.y.z.jar\n"
                    + "\n"
                    + "core-io-x.y.z.jar\n"
                    + "\n"
                    + "rxjava-x.y.z.jar\n"
                    + "\n"
                    + "See the documentation: https://neo4j-contrib.github.io/neo4j-apoc-procedures/#_interacting_with_couchbase";

    /**
     * Retrieves a document by its unique ID.
     * <p/>
     * Example:
     * <code>call apoc.couchbase.get('localhost', 'default', 'artist:vincent_van_gogh') yield id, expiry, cas, mutationToken, content</code>
     *
     * @param hostOrKey  a URI to use when connecting to the cluster reference or a configuration key
     * @param bucket     the bucket to open; if null is passed then it's used the "default"
     *                   bucket
     * @param documentId the unique ID of the document
     * @return the found {@link CouchbaseJsonDocument} or null if not found
     */
    @Procedure
    @Description(
            "apoc.couchbase.get(hostOrKey, bucket, documentId) yield id, expiry, cas, mutationToken, content - retrieves a couchbase json document by its unique ID.")
    public Stream<CouchbaseJsonDocument> get(
            @Name("hostOrKey") String hostOrKey,
            @Name("bucket") String bucket,
            @Name("documentId") String documentId,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        try (CouchbaseConnection couchbaseConnection = getCouchbaseConnection(hostOrKey, bucket, config)) {
            return Stream.ofNullable(couchbaseConnection.get(documentId));
        }
    }

    /**
     * Check whether a document with the given ID does exist.
     * <p/>
     * Example:
     * <code>CALL apoc.couchbase.exists('localhost', 'default', 'artist:vincent_van_gogh') yield value</code>
     *
     * @param hostOrKey  a URI to use when connecting to the cluster reference or a configuration key
     * @param bucket     the bucket to open; if null is passed then it's used the "default"
     *                   bucket
     * @param documentId the unique ID of the document
     * @return true if it exists, false otherwise.
     */
    @Procedure
    @Description(
            "apoc.couchbase.exists(hostOrKey, bucket, documentId) yield value - check whether a couchbase json document with the given ID does exist.")
    public Stream<BooleanResult> exists(
            @Name("hostOrKey") String hostOrKey,
            @Name("bucket") String bucket,
            @Name("documentId") String documentId,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        try (CouchbaseConnection couchbaseConnection = getCouchbaseConnection(hostOrKey, bucket, config)) {
            return Stream.ofNullable(new BooleanResult(couchbaseConnection.exists(documentId)));
        }
    }

    /**
     * Insert a document if it does not exist already.
     * <p/>
     * Example:
     * <code>CALL apoc.couchbase.insert('localhost', 'default', 'artist:vincent_van_gogh', '{"firstName":"Vincent","secondName":"Willem","lastName":"Van Gogh","notableWorks":["Starry Night","Sunflowers","Bedroom in Arles","Portrait of Dr Gachet","Sorrow"]}') yield id, expiry, cas, mutationToken, content</code>
     *
     * @param hostOrKey  a URI to use when connecting to the cluster reference or a configuration key
     * @param bucket     the bucket to open; if null is passed then it's used the "default"
     *                   bucket
     * @param documentId the unique ID of the document
     * @param json       the JSON String representing the document to store
     * @return the newly created document
     */
    @Procedure
    @Description(
            "apoc.couchbase.insert(hostOrKey, bucket, documentId, jsonDocument) yield id, expiry, cas, mutationToken, content - insert a couchbase json document with its unique ID.")
    public Stream<CouchbaseJsonDocument> insert(
            @Name("hostOrKey") String hostOrKey,
            @Name("bucket") String bucket,
            @Name("documentId") String documentId,
            @Name("json") String json,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        try (CouchbaseConnection couchbaseConnection = getCouchbaseConnection(hostOrKey, bucket, config)) {
            return Stream.ofNullable(couchbaseConnection.insert(documentId, json));
        }
    }

    /**
     * Insert or overwrite a document.
     * <p/>
     * Example:
     * <code>CALL apoc.couchbase.upsert('localhost', 'default', 'artist:vincent_van_gogh', '{"firstName":"Vincent","secondName":"Willem","lastName":"Van Gogh","notableWorks":["Starry Night","Sunflowers","Bedroom in Arles","Portrait of Dr Gachet","Sorrow"]}') yield id, expiry, cas, mutationToken, content</code>
     *
     * @param hostOrKey  a URI to use when connecting to the cluster reference or a configuration key
     * @param bucket     the bucket to open; if null is passed then it's used the "default"
     *                   bucket
     * @param documentId the unique ID of the document
     * @param json       the JSON String representing the document to store
     * @return the newly created or overwritten document or null in case of exception
     */
    @Procedure
    @Description(
            "apoc.couchbase.upsert(hostOrKey, bucket, documentId, jsonDocument) yield id, expiry, cas, mutationToken, content - insert or overwrite a couchbase json document with its unique ID.")
    public Stream<CouchbaseJsonDocument> upsert(
            @Name("hostOrKey") String hostOrKey,
            @Name("bucket") String bucket,
            @Name("documentId") String documentId,
            @Name("json") String json,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        try (CouchbaseConnection couchbaseConnection = getCouchbaseConnection(hostOrKey, bucket, config)) {
            return Stream.ofNullable(couchbaseConnection.upsert(documentId, json));
        }
    }

    /**
     * Append a document's content to an existing one.
     * <p/>
     * Example:
     * <code>CALL apoc.couchbase.append('localhost', 'default', 'artist:vincent_van_gogh', 'hello world'.getBytes()) yield id, expiry, cas, mutationToken, content</code>
     *
     * @param hostOrKey  a URI to use when connecting to the cluster reference or a configuration key
     * @param bucket     the bucket to open; if null is passed then it's used the "default"
     *                   bucket
     * @param documentId the unique ID of the document
     * @param content    the byte[] representing the document to append
     * @return the updated document or null in case of exception
     */
    @Procedure
    @Description(
            "apoc.couchbase.append(hostOrKey, bucket, documentId, content) yield id, expiry, cas, mutationToken, content - append a couchbase json document to an existing one.")
    public Stream<CouchbaseByteArrayDocument> append(
            @Name("hostOrKey") String hostOrKey,
            @Name("bucket") String bucket,
            @Name("documentId") String documentId,
            @Name("content") byte[] content,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        try (CouchbaseConnection couchbaseConnection = getCouchbaseConnection(hostOrKey, bucket, config)) {
            return Stream.ofNullable(couchbaseConnection.append(documentId, content));
        }
    }

    /**
     * Prepend a document's content to an existing one.
     * <p/>
     * Example:
     * <code>CALL apoc.couchbase.prepend('localhost', 'default', 'artist:vincent_van_gogh', 'hello world'.getBytes()) yield id, expiry, cas, mutationToken, content</code>
     *
     * @param hostOrKey  a URI to use when connecting to the cluster reference or a configuration key
     * @param bucket     the bucket to open; if null is passed then it's used the "default"
     *                   bucket
     * @param documentId the unique ID of the document
     * @param content    the byte[] representing the document to prepend
     * @return the updated document or null in case of exception
     */
    @Procedure
    @Description(
            "apoc.couchbase.prepend(hostOrKey, bucket, documentId, content) yield id, expiry, cas, mutationToken, content - prepend a couchbase json document to an existing one.")
    public Stream<CouchbaseByteArrayDocument> prepend(
            @Name("hostOrKey") String hostOrKey,
            @Name("bucket") String bucket,
            @Name("documentId") String documentId,
            @Name("content") byte[] content,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        try (CouchbaseConnection couchbaseConnection = getCouchbaseConnection(hostOrKey, bucket, config)) {
            return Stream.ofNullable(couchbaseConnection.prepend(documentId, content));
        }
    }

    /**
     * Removes the document identified by its unique ID.
     * <p/>
     * Example:
     * <code>CALL apoc.couchbase.remove(['localhost'], 'default', 'artist:vincent_van_gogh') yield id, expiry, cas, mutationToken, content</code>
     *
     * @param hostOrKey  a URI to use when connecting to the cluster reference or a configuration key
     * @param bucket     the bucket to open; if null is passed then it's used the "default"
     *                   bucket
     * @param documentId the unique ID of the document
     * @return the removed document
     */
    @Procedure
    @Description(
            "apoc.couchbase.remove(hostOrKey, bucket, documentId) yield id, expiry, cas, mutationToken, content - remove the couchbase json document identified by its unique ID.")
    public Stream<CouchbaseJsonDocument> remove(
            @Name("hostOrKey") String hostOrKey,
            @Name("bucket") String bucket,
            @Name("documentId") String documentId,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        try (CouchbaseConnection couchbaseConnection = getCouchbaseConnection(hostOrKey, bucket, config)) {
            return Stream.ofNullable(couchbaseConnection.remove(documentId));
        }
    }

    /**
     * Replace the content of the document identified by its unique
     * ID.
     * <p/>
     * Example:
     * <code>CALL apoc.couchbase.replace('localhost', 'default', 'artist:vincent_van_gogh', '{"firstName":"Vincent","secondName":"Willem","lastName":"Van Gogh","notableWorks":["Starry Night","Sunflowers","Bedroom in Arles","Portrait of Dr Gachet","Sorrow"],"placeOfBirth":"Zundert","placeOfDeath":" Auvers-sur-Oise"}') yield id, expiry, cas, mutationToken, content</code>
     *
     * @param hostOrKey  a URI to use when connecting to the cluster reference or a configuration key
     * @param bucket     the bucket to open; if null is passed then it's used the "default"
     *                   bucket
     * @param documentId the unique ID of the document
     * @param json       the JSON String representing the document to prepend
     * @return the replaced document
     */
    @Procedure
    @Description(
            "apoc.couchbase.replace(hostOrKey, bucket, documentId, jsonDocument) yield id, expiry, cas, mutationToken, content - replace the content of the couchbase json document identified by its unique ID.")
    public Stream<CouchbaseJsonDocument> replace(
            @Name("hostOrKey") String hostOrKey,
            @Name("bucket") String bucket,
            @Name("documentId") String documentId,
            @Name("json") String json,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        try (CouchbaseConnection couchbaseConnection = getCouchbaseConnection(hostOrKey, bucket, config)) {
            return Stream.ofNullable(couchbaseConnection.replace(documentId, json));
        }
    }

    /**
     * Executes a plain un-parameterized N1QL statement.
     * <p/>
     * Example:
     * <code>CALL apoc.couchbase.query('localhost', 'default', 'select * from default where lastName = "Van Gogh"']) yield queryResult</code>
     *
     * @param hostOrKey a URI to use when connecting to the cluster reference or a configuration key
     * @param bucket    the bucket to open; if null is passed then it's used the "default"
     *                  bucket
     * @param statement the raw statement string to execute
     * @return the list of JSON Objects retrieved by this query in the form of a {@link CouchbaseQueryResult}
     */
    @Procedure
    @Description(
            "apoc.couchbase.query(hostOrKey, bucket, statement) yield queryResult - executes a plain un-parameterized N1QL statement.")
    public Stream<CouchbaseQueryResult> query(
            @Name("hostOrKey") String hostOrKey,
            @Name("bucket") String bucket,
            @Name("statement") String statement,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        try (CouchbaseConnection couchbaseConnection = getCouchbaseConnection(hostOrKey, bucket, config)) {
            return Stream.ofNullable(couchbaseConnection.executeStatement(statement));
        }
    }

    /**
     * Executes a N1QL statement with positional parameters.
     * <p/>
     * Example:
     * <code>CALL apoc.couchbase.posParamsQuery('localhost', 'default', 'select * from default where lastName = $1', ['Van Gogh']) yield queryResult</code>
     *
     * @param hostOrKey a URI to use when connecting to the cluster reference or a configuration key
     * @param bucket    the bucket to open; if null is passed then it's used the "default"
     *                  bucket
     * @param statement the raw statement string to execute (containing positional
     *                  placeholders: $1, $2, ...)
     * @param params    the values for the positional placeholders in statement
     * @return the list of JSON Objects retrieved by this query in the form of a {@link CouchbaseQueryResult}
     */
    @Procedure
    @Description(
            "apoc.couchbase.posParamsQuery(hostOrKey, bucket, statement, params) yield queryResult - executes a N1QL statement with positional parameters.")
    public Stream<CouchbaseQueryResult> posParamsQuery(
            @Name("hostOrKey") String hostOrKey,
            @Name("bucket") String bucket,
            @Name("statement") String statement,
            @Name("params") List<Object> params,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        try (CouchbaseConnection couchbaseConnection = getCouchbaseConnection(hostOrKey, bucket, config)) {
            CouchbaseQueryResult result = couchbaseConnection.executeParameterizedStatement(statement, params);
            return Stream.ofNullable(result);
        }
    }

    /**
     * Executes a N1QL statement with named parameters.
     * <p/>
     * Example:
     * <code>CALL apoc.couchbase.namedParamsQuery('localhost', 'default', 'select * from default where lastName = $lastName', ['lastName'], ['Van Gogh']) yield queryResult</code>
     *
     * @param hostOrKey   a URI to use when connecting to the cluster reference or a configuration key
     * @param bucket      the bucket to open; if null is passed then it's used the "default"
     *                    bucket
     * @param statement   the raw statement string to execute (containing named
     *                    placeholders: $param1, $param2, ...)
     * @param paramNames  the placeholders' names in statement
     * @param paramValues the values for the named placeholders in statement
     * @return the list of JSON Objects retrieved by this query in the form of a {@link CouchbaseQueryResult}
     */
    @Procedure
    @Description(
            "apoc.couchbase.namedParamsQuery(hostkOrKey, bucket, statement, paramNames, paramValues) yield queryResult - executes a N1QL statement with named parameters.")
    public Stream<CouchbaseQueryResult> namedParamsQuery(
            @Name("hostOrKey") String hostOrKey,
            @Name("bucket") String bucket,
            @Name("statement") String statement,
            @Name("paramNames") List<String> paramNames,
            @Name("paramValues") List<Object> paramValues,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        try (CouchbaseConnection couchbaseConnection = getCouchbaseConnection(hostOrKey, bucket, config)) {
            final CouchbaseQueryResult result =
                    couchbaseConnection.executeParameterizedStatement(statement, paramNames, paramValues);
            return Stream.ofNullable(result);
        }
    }

    private CouchbaseConnection getCouchbaseConnection(String hostOrKey, String bucket, Map<String, Object> configMap) {
        try {
            CouchbaseConfig config = new CouchbaseConfig(configMap);
            return CouchbaseManager.getConnection(hostOrKey, bucket, config);
        } catch (NoClassDefFoundError e) {
            throw new MissingDependencyException(COUCHBASE_MISSING_DEPS_ERROR);
        }
    }
}
