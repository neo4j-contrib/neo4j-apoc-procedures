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
package apoc.es;

import static apoc.ApocConfig.apocConfig;
import static org.junit.Assert.*;

import apoc.util.JsonUtil;
import apoc.util.TestUtil;
import apoc.util.Util;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;
import org.testcontainers.elasticsearch.ElasticsearchContainer;

/**
 * @author mh
 * @since 21.05.16
 */
public class ElasticSearchTest {

    private static final String URL_CONF = "apoc.es.url";
    private static String HTTP_HOST_ADDRESS;
    private static String HTTP_URL_ADDRESS;

    public static ElasticsearchContainer elastic;

    private static final String ES_INDEX = "test-index";

    private static final String ES_TYPE = "test-type";

    private static final String ES_ID = UUID.randomUUID().toString();

    private static final String HOST = "localhost";

    private static final String DOCUMENT =
            "{\"name\":\"Neo4j\",\"company\":\"Neo Technology\",\"description\":\"Awesome stuff with a graph database\"}";

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    private static Map<String, Object> defaultParams = Util.map("index", ES_INDEX, "type", ES_TYPE, "id", ES_ID);
    private static Map<String, Object> paramsWithBasicAuth;
    private static Map<String, Object> basicAuthHeader;

    // We need a reference to the class implementing the procedures
    private final ElasticSearch es = new ElasticSearch();
    private static final Configuration JSON_PATH_CONFIG = Configuration.builder()
            .options(Option.DEFAULT_PATH_LEAF_TO_NULL, Option.SUPPRESS_EXCEPTIONS)
            .build();

    @BeforeClass
    public static void setUp() throws Exception {
        final String password = "myPassword";
        elastic = new ElasticsearchContainer();
        elastic.start();
        defaultParams.put("host", elastic.getHttpHostAddress());

        String httpHostAddress = elastic.getHttpHostAddress();
        HTTP_HOST_ADDRESS = String.format("elastic:%s@%s", password, httpHostAddress);

        HTTP_URL_ADDRESS = "http://" + HTTP_HOST_ADDRESS;

        defaultParams.put("host", HTTP_HOST_ADDRESS);
        defaultParams.put("url", HTTP_URL_ADDRESS);

        // We can authenticate to elastic using the url `<elastic>:<password>@<hostAddress>`
        // or via Basic authentication, i.e. using the url `<hostAddress>` together with the header `Authorization:
        // Basic <token>`
        // where <token> is Base64(<username>:<password>)
        String token = Base64.getEncoder().encodeToString(("elastic:" + password).getBytes());
        basicAuthHeader = Map.of("Authorization", "Basic " + token);

        paramsWithBasicAuth = new HashMap<>(defaultParams);
        paramsWithBasicAuth.put("host", elastic.getHttpHostAddress());
        paramsWithBasicAuth.put("headers", basicAuthHeader);

        TestUtil.registerProcedure(db, ElasticSearch.class);
        insertDocuments();
    }

    private static String getRawProcedureUrl(String id) {
        return ES_INDEX + "/" + ES_TYPE + "/" + id + "?refresh=true";
    }

    @AfterClass
    public static void tearDown() {
        elastic.stop();
        db.shutdown();
    }

    /**
     * Default params (host, index, type, id) + payload
     *
     * @param payload
     * @return
     */
    private static Map<String, Object> createDefaultProcedureParametersWithPayloadAndId(String payload, String id) {
        try {
            Map mapPayload = JsonUtil.OBJECT_MAPPER.readValue(payload, Map.class);
            return addPayloadAndIdToParams(defaultParams, mapPayload, id);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static Map<String, Object> addPayloadAndIdToParams(Map<String, Object> params, Object payload, String id) {
        return Util.merge(params, Util.map("payload", payload, "id", id));
    }

    private static void insertDocuments() {
        Map<String, Object> params = createDefaultProcedureParametersWithPayloadAndId(
                "{\"procedurePackage\":\"es\",\"procedureName\":\"get\",\"procedureDescription\":\"perform a GET operation to ElasticSearch\"}",
                UUID.randomUUID().toString());
        TestUtil.testCall(
                db, "CALL apoc.es.put($host,$index,$type,$id,'refresh=true',$payload) yield value", params, r -> {
                    Object created = extractValueFromResponse(r, "$.result");
                    assertEquals("created", created);
                });

        params = createDefaultProcedureParametersWithPayloadAndId(
                "{\"procedurePackage\":\"es\",\"procedureName\":\"post\",\"procedureDescription\":\"perform a POST operation to ElasticSearch\"}",
                UUID.randomUUID().toString());
        TestUtil.testCall(
                db, "CALL apoc.es.put($host,$index,$type,$id,'refresh=true',$payload) yield value", params, r -> {
                    Object created = extractValueFromResponse(r, "$.result");
                    assertEquals("created", created);
                });

        params = createDefaultProcedureParametersWithPayloadAndId(DOCUMENT, ES_ID);
        TestUtil.testCall(
                db, "CALL apoc.es.put($host,$index,$type,$id,'refresh=true',$payload) yield value", params, r -> {
                    Object created = extractValueFromResponse(r, "$.result");
                    assertEquals("created", created);
                });
    }

    private static Object extractValueFromResponse(Map response, String jsonPath) {
        Object jsonResponse = response.get("value");
        assertNotNull(jsonResponse);

        String json = JsonPath.parse(jsonResponse).jsonString();
        Object value = JsonPath.parse(json, JSON_PATH_CONFIG).read(jsonPath);

        return value;
    }

    @Test
    public void testStats() throws Exception {
        TestUtil.testCall(db, "CALL apoc.es.stats($host)", defaultParams, commonEsStatsConsumer());
    }

    @Test
    public void testStatsWithAuthHeader() {
        TestUtil.testCall(
                db, "CALL apoc.es.stats($host, {headers: $headers})", paramsWithBasicAuth, commonEsStatsConsumer());
    }

    /**
     * Simple get request for document retrieval
     * http://localhost:9200/test-index/test-type/ee6749ff-b836-4529-88e9-3105675d625a
     *
     * @throws Exception
     */
    @Test
    public void testGetWithQueryNull() throws Exception {
        TestUtil.testCall(
                db,
                "CALL apoc.es.get($host,$index,$type,$id,null,null) yield value",
                defaultParams,
                commonEsGetConsumer());
    }

    @Test
    public void testProceduresWithUrl() {
        TestUtil.testCall(db, "CALL apoc.es.stats($url)", defaultParams, commonEsStatsConsumer());

        TestUtil.testCall(
                db,
                "CALL apoc.es.get($url,$index,$type,$id,null,null) yield value",
                defaultParams,
                commonEsGetConsumer());
    }

    @Test
    public void testProceduresWithUrlAndHeaders() {
        TestUtil.testCall(
                db, "CALL apoc.es.stats($url, {headers: $headers})", paramsWithBasicAuth, commonEsStatsConsumer());

        TestUtil.testCall(
                db,
                "CALL apoc.es.get($url,$index,$type,$id,null,null) yield value",
                paramsWithBasicAuth,
                commonEsGetConsumer());
    }

    @Test
    public void testGetRowProcedure() {
        Map<String, Object> params = Map.of("url", HTTP_URL_ADDRESS, "suffix", getRawProcedureUrl(ES_ID));

        TestUtil.testCall(db, "CALL apoc.es.getRaw($url,$suffix, null)", params, commonEsGetConsumer());
    }

    @Test
    public void testGetRowProcedureWithAuthHeader() {
        Map<String, Object> params = Map.of(
                "url", elastic.getHttpHostAddress(), "suffix", getRawProcedureUrl(ES_ID), "headers", basicAuthHeader);

        TestUtil.testCall(
                db, "CALL apoc.es.getRaw($url, $suffix, null, {headers: $headers})", params, commonEsGetConsumer());
    }

    @Test
    public void testProceduresWithUrlKeyConfOverridingGenericUrlConf() {
        apocConfig().setProperty("apoc.es.customKey.url", HTTP_URL_ADDRESS);
        apocConfig().setProperty(URL_CONF, "wrongUrl");

        TestUtil.testCall(db, "CALL apoc.es.stats('customKey')", commonEsStatsConsumer());

        TestUtil.testCall(
                db,
                "CALL apoc.es.get('customKey',$index,$type,$id,null,null) yield value",
                defaultParams,
                commonEsGetConsumer());

        apocConfig().getConfig().clearProperty(URL_CONF);
    }

    @Test
    public void testProceduresWithUrlKeyConf() {
        apocConfig().setProperty("apoc.es.myUrlKey.url", HTTP_URL_ADDRESS);

        TestUtil.testCall(db, "CALL apoc.es.stats('myUrlKey')", commonEsStatsConsumer());

        TestUtil.testCall(
                db,
                "CALL apoc.es.get('myUrlKey',$index,$type,$id,null,null) yield value",
                defaultParams,
                commonEsGetConsumer());
    }

    @Test
    public void testProceduresWithHostKeyConf() {
        apocConfig().setProperty("apoc.es.myHostKey.host", HTTP_HOST_ADDRESS);

        TestUtil.testCall(db, "CALL apoc.es.stats('myHostKey')", commonEsStatsConsumer());

        TestUtil.testCall(
                db,
                "CALL apoc.es.get('myHostKey',$index,$type,$id,null,null) yield value",
                defaultParams,
                commonEsGetConsumer());
    }

    /**
     * Simple get request for document retrieval but we also send multiple commands (as a Map) to ES
     * http://localhost:9200/test-index/test-type/4fa40c40-db89-4761-b6a3-75f0015db059?_source_includes=name&_source_excludes=description
     *
     * @throws Exception
     */
    @Test
    public void testGetWithQueryAsMapMultipleParams() throws Exception {
        TestUtil.testCall(
                db,
                "CALL apoc.es.get($host,$index,$type,$id,{_source_includes:'name',_source_excludes:'description'},null) yield value",
                defaultParams,
                commonEsGetConsumer());
    }

    /**
     * Simple get request for document retrieval but we also send a single command (as a Map) to ES
     * http://localhost:9200/test-index/test-type/4fa40c40-db89-4761-b6a3-75f0015db059?_source_includes=name
     *
     * @throws Exception
     */
    @Test
    public void testGetWithQueryAsMapSingleParam() throws Exception {
        TestUtil.testCall(
                db,
                "CALL apoc.es.get($host,$index,$type,$id,{_source_includes:'name'},null) yield value",
                defaultParams,
                commonEsGetConsumer());
    }

    /**
     * Simple get request for document retrieval but we also send multiple commands (as a string) to ES
     * http://localhost:9200/test-index/test-type/4fa40c40-db89-4761-b6a3-75f0015db059?_source_includes=name&_source_excludes=description
     *
     * @throws Exception
     */
    @Test
    public void testGetWithQueryAsStringMultipleParams() throws Exception {
        TestUtil.testCall(
                db,
                "CALL apoc.es.get($host,$index,$type,$id,'_source_includes=name&_source_excludes=description',null) yield value",
                defaultParams,
                commonEsGetConsumer());
    }

    /**
     * Simple get request for document retrieval but we also send a single command (as a string) to ES
     * http://localhost:9200/test-index/test-type/4fa40c40-db89-4761-b6a3-75f0015db059?_source_includes=name
     *
     * @throws Exception
     */
    @Test
    public void testGetWithQueryAsStringSingleParam() throws Exception {
        TestUtil.testCall(
                db,
                "CALL apoc.es.get($host,$index,$type,$id,'_source_includes=name',null) yield value",
                defaultParams,
                commonEsGetConsumer());
    }

    /**
     * We want to search our document by name --> /test-index/test-type/_search?
     * This test uses a plain string to query ES
     */
    @Test
    public void testSearchWithQueryNull() throws Exception {
        TestUtil.testCall(db, "CALL apoc.es.query($host,$index,$type,null,null) yield value", defaultParams, r -> {
            Object hits = extractValueFromResponse(r, "$.hits.hits");
            assertEquals(3, ((List) hits).size());
        });
    }

    /**
     * We want to search our document by name --> /test-index/test-type/_search?q=name:Neo4j
     * This test uses a plain string to query ES
     */
    @Test
    public void testSearchWithQueryAsAString() throws Exception {
        TestUtil.testCall(
                db, "CALL apoc.es.query($host,$index,$type,'q=name:Neo4j',null) yield value", defaultParams, r -> {
                    Object name = extractValueFromResponse(r, "$.hits.hits[0]._source.name");
                    assertEquals("Neo4j", name);
                });
    }

    @Test
    public void testSearchWithQueryAsAStringAndHeader() throws Exception {
        TestUtil.testCall(
                db,
                "CALL apoc.es.query($host, $index, $type, 'q=name:Neo4j', null, {headers: $headers}) yield value",
                paramsWithBasicAuth,
                r -> {
                    Object name = extractValueFromResponse(r, "$.hits.hits[0]._source.name");
                    assertEquals("Neo4j", name);
                });
    }

    /**
     * We want to search our document by name --> /test-index/test-type/_search?q=name:*
     * This test uses a plain string to query ES
     */
    @Test
    public void testFullSearchWithQueryAsAString() throws Exception {
        TestUtil.testCall(
                db, "CALL apoc.es.query($host,$index,$type,'q=name:*',null) yield value", defaultParams, r -> {
                    Object name = extractValueFromResponse(r, "$.hits.hits[0]._source.name");
                    assertEquals("Neo4j", name);
                });
    }

    /**
     * We want to search our document by name --> /test-index/test-type/_search?q=procedureName:get
     * This test uses a plain string to query ES
     */
    @Test
    public void testFullSearchWithQueryAsAStringWithEquals() throws Exception {
        TestUtil.testCall(
                db,
                "CALL apoc.es.query($host,$index,$type,'q=procedureName:get',null) yield value",
                defaultParams,
                r -> {
                    Object name = extractValueFromResponse(r, "$.hits.hits[0]._source.procedureName");
                    assertEquals("get", name);
                });
    }

    /**
     * We want to search our document by name --> /test-index/test-type/_search?size=1&scroll=1m&_source=true
     * This test uses a plain string to query ES
     */
    @Test
    public void testFullSearchWithOtherParametersAsAString() throws Exception {
        TestUtil.testCall(
                db,
                "CALL apoc.es.query($host,$index,$type,'size=1&scroll=1m&_source=true&q=procedureName:get',null) yield value",
                defaultParams,
                r -> {
                    Object hits = extractValueFromResponse(r, "$.hits.hits");
                    assertEquals(1, ((List) hits).size());
                    Object name = extractValueFromResponse(r, "$.hits.hits[0]._source.procedureName");
                    assertEquals("get", name);
                });
    }

    /**
     * We create a document with a field tags that is a collection of a single element "awesome".
     * Then we update the same field with the collection ["beautiful"]
     * and we retrieve the document in order to verify the update.
     * <p>
     * http://localhost:9200/test-index/test-type/f561c1c5-4092-4c5d-98a6-5ea2b3417415/_update
     */
    @Test
    public void testPutUpdateDocument() throws IOException {
        Map<String, Object> doc = JsonUtil.OBJECT_MAPPER.readValue(DOCUMENT, Map.class);
        doc.put("tags", Arrays.asList("awesome"));
        Map<String, Object> params =
                createDefaultProcedureParametersWithPayloadAndId(JsonUtil.OBJECT_MAPPER.writeValueAsString(doc), ES_ID);
        TestUtil.testCall(
                db, "CALL apoc.es.put($host,$index,$type,$id,'refresh=true',$payload) yield value", params, r -> {
                    Object updated = extractValueFromResponse(r, "$.result");
                    assertEquals("updated", updated);
                });

        TestUtil.testCall(db, "CALL apoc.es.get($host,$index,$type,$id,null,null) yield value", params, r -> {
            Object tag = extractValueFromResponse(r, "$._source.tags[0]");
            assertEquals("awesome", tag);
        });
    }

    @Test
    public void testPutUpdateDocumentWithAuthHeader() throws IOException {
        String tags = UUID.randomUUID().toString();

        Map<String, Object> doc = JsonUtil.OBJECT_MAPPER.readValue(DOCUMENT, Map.class);
        doc.put("tags", Arrays.asList(tags));
        Map<String, Object> params = addPayloadAndIdToParams(paramsWithBasicAuth, doc, ES_ID);
        TestUtil.testCall(
                db,
                "CALL apoc.es.put($host,$index,$type,$id,'refresh=true',$payload, {headers: $headers}) yield value",
                params,
                r -> {
                    Object result = extractValueFromResponse(r, "$.result");
                    assertEquals("updated", result);
                });

        TestUtil.testCall(
                db,
                "CALL apoc.es.get($host, $index, $type, $id, null, null, {headers: $headers}) yield value",
                params,
                r -> {
                    Object actualTags = extractValueFromResponse(r, "$._source.tags[0]");
                    assertEquals(tags, actualTags);
                });
    }

    @Test
    public void testPostRawCreateDocument() throws IOException {
        String index = UUID.randomUUID().toString();
        String type = UUID.randomUUID().toString();
        String id = UUID.randomUUID().toString();
        Map payload = JsonUtil.OBJECT_MAPPER.readValue("{\"ajeje\":\"Brazorf\"}", Map.class);
        Map params = Util.map(
                "host",
                HTTP_HOST_ADDRESS,
                "index",
                index,
                "suffix",
                index,
                "type",
                type,
                "payload",
                payload,
                "suffixDelete",
                index,
                "suffixPost",
                index + "/" + type + "/" + id + "?refresh=true",
                "id",
                id);

        TestUtil.testCall(db, "CALL apoc.es.postRaw($host, $suffixPost, $payload) yield value", params, r -> {
            Object result = extractValueFromResponse(r, "$.result");
            assertEquals("created", result);
        });

        TestUtil.testCall(db, "CALL apoc.es.get($host, $index, $type, $id, null, null) yield value", params, r -> {
            Object response = extractValueFromResponse(r, "$._source.ajeje");
            assertEquals("Brazorf", response);
        });

        TestUtil.testCall(db, "CALL apoc.es.delete($host, $index, $type, $id, 'refresh=true')", params, r -> {
            Object result = extractValueFromResponse(r, "$.result");
            assertEquals("deleted", result);
        });
    }

    @Test
    public void testPostCreateDocumentWithAuthHeader() throws IOException {
        String index = UUID.randomUUID().toString();
        String type = UUID.randomUUID().toString();
        Map payload = JsonUtil.OBJECT_MAPPER.readValue("{\"ajeje\":\"Brazorf\"}", Map.class);
        Map params = Util.map(
                "host",
                elastic.getHttpHostAddress(),
                "index",
                index,
                "type",
                type,
                "payload",
                payload,
                "suffix",
                index,
                "headers",
                basicAuthHeader);

        AtomicReference<String> id = new AtomicReference<>();
        TestUtil.testCall(
                db,
                "CALL apoc.es.post($host,$index,$type,'refresh=true', $payload, {headers: $headers}) yield value",
                params,
                r -> {
                    Object result = extractValueFromResponse(r, "$.result");
                    assertEquals("created", result);

                    id.set((String) ((Map) r.get("value")).get("_id"));
                });

        params.put("id", id.get());

        TestUtil.testCall(
                db,
                "CALL apoc.es.get($host, $index, $type, $id, null, null, {headers: $headers}) yield value",
                params,
                r -> {
                    Object actual = extractValueFromResponse(r, "$._source.ajeje");
                    assertEquals("Brazorf", actual);
                });

        TestUtil.testCall(
                db,
                "CALL apoc.es.delete($host, $index, $type, $id, 'refresh=true', {headers: $headers})",
                params,
                r -> {
                    Object result = extractValueFromResponse(r, "$.result");
                    assertEquals("deleted", result);
                });
    }

    /**
     * We want to to search our document by name --> /test-index/test-type/_search?q=name:Neo4j
     * This test uses a Map to query ES
     */
    @Test
    public void testSearchWithQueryAsAMap() {
        TestUtil.testCall(
                db,
                "CALL apoc.es.query($host,$index,$type,null,{query: {match: {name: 'Neo4j'}}}) yield value",
                defaultParams,
                r -> {
                    Object name = extractValueFromResponse(r, "$.hits.hits[0]._source.name");
                    assertEquals("Neo4j", name);
                });
    }

    @Test
    public void testGetQueryUrlShouldBeTheSameAsOldFormatting() {
        String index = ES_INDEX;
        String type = ES_TYPE;
        String id = ES_ID;
        Map<String, String> query = new HashMap<>();
        query.put("name", "get");

        String host = HOST;
        String hostUrl = es.getElasticSearchUrl(host);

        String queryUrl = hostUrl
                + String.format(
                        "/%s/%s/%s?%s",
                        index == null ? "_all" : index,
                        type == null ? "_all" : type,
                        id == null ? "" : id,
                        es.toQueryParams(query));

        assertEquals(queryUrl, es.getQueryUrl(host, index, type, id, query));
    }

    @Test
    public void testGetQueryUrlShouldNotHaveTrailingQuestionMarkIfQueryIsNull() {
        String index = ES_INDEX;
        String type = ES_TYPE;
        String id = ES_TYPE;

        String host = HOST;
        String hostUrl = es.getElasticSearchUrl(host);
        String queryUrl = hostUrl
                + String.format(
                        "/%s/%s/%s?%s",
                        index == null ? "_all" : index,
                        type == null ? "_all" : type,
                        id == null ? "" : id,
                        es.toQueryParams(null));

        // First we test the older version against the newest one
        assertNotEquals(queryUrl, es.getQueryUrl(host, index, type, id, null));
        assertTrue(!es.getQueryUrl(host, index, type, id, null).endsWith("?"));
    }

    @Test
    public void testGetQueryUrlShouldNotHaveTrailingQuestionMarkIfQueryIsEmpty() {
        String index = ES_INDEX;
        String type = ES_TYPE;
        String id = ES_ID;

        String host = HOST;
        String hostUrl = es.getElasticSearchUrl(host);
        String queryUrl = hostUrl
                + String.format(
                        "/%s/%s/%s?%s",
                        index == null ? "_all" : index,
                        type == null ? "_all" : type,
                        id == null ? "" : id,
                        es.toQueryParams(new HashMap<String, String>()));

        // First we test the older version against the newest one
        assertNotEquals(queryUrl, es.getQueryUrl(host, index, type, id, new HashMap<String, String>()));
        assertTrue(!es.getQueryUrl(host, index, type, id, new HashMap<String, String>())
                .endsWith("?"));
    }

    private static Consumer<Map<String, Object>> commonEsGetConsumer() {
        return r -> {
            Object name = extractValueFromResponse(r, "$._source.name");
            assertEquals("Neo4j", name);
        };
    }

    private static Consumer<Map<String, Object>> commonEsStatsConsumer() {
        return r -> {
            assertNotNull(r.get("value"));
            Object numOfDocs = extractValueFromResponse(r, "$._all.total.docs.count");
            assertEquals(3, numOfDocs);
        };
    }
}
