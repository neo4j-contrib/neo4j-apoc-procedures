package apoc.full.it.es;

import static apoc.ApocConfig.apocConfig;
import static apoc.util.MapUtil.map;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import apoc.es.ElasticSearch;
import apoc.util.JsonUtil;
import apoc.util.TestUtil;
import apoc.util.Util;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.junit.AfterClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;
import org.testcontainers.elasticsearch.ElasticsearchContainer;

/**
 * @author mh
 * @since 21.05.16
 */
public abstract class ElasticSearchTest {

    private static final String URL_CONF = "apoc.es.url";
    static String HTTP_HOST_ADDRESS;
    static String HTTP_URL_ADDRESS;

    public static ElasticsearchContainer elastic;

    static final String ES_INDEX = "test-index";

    abstract String getEsType();

    static final String ES_ID = UUID.randomUUID().toString();

    private static final String DOCUMENT =
            "{\"name\":\"Neo4j\",\"company\":\"Neo Technology\",\"description\":\"Awesome stuff with a graph database\"}";

    static final String password = "myPassword";
    static Map<String, Object> basicAuthHeader =
            Map.of("Authorization", "Basic " + Base64.getEncoder().encodeToString(("elastic:" + password).getBytes()));

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    static Map<String, Object> paramsWithBasicAuth;

    private static final Configuration JSON_PATH_CONFIG = Configuration.builder()
            .options(Option.DEFAULT_PATH_LEAF_TO_NULL, Option.SUPPRESS_EXCEPTIONS)
            .build();

    static void getElasticContainer(String tag, Map<String, String> envMap, Map<String, Object> params)
            throws JsonProcessingException {

        elastic = new ElasticsearchContainer("docker.elastic.co/elasticsearch/elasticsearch:" + tag)
                .withPassword(password)
                .withEnv(envMap);
        elastic.start();

        String httpHostAddress = elastic.getHttpHostAddress();
        HTTP_HOST_ADDRESS = String.format("elastic:%s@%s", password, httpHostAddress);

        HTTP_URL_ADDRESS = "http://" + HTTP_HOST_ADDRESS;

        params.put("host", elastic.getHttpHostAddress());
        params.put("url", "http://" + elastic.getHttpHostAddress());
        paramsWithBasicAuth = params;
        TestUtil.registerProcedure(db, ElasticSearch.class);
        insertDocuments();
    }

    static String getRawProcedureUrl(String id, String type) {
        return ES_INDEX + "/" + type + "/" + id + "?refresh=true";
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
    static Map<String, Object> createDefaultProcedureParametersWithPayloadAndId(String payload, String id) {
        try {
            Map mapPayload = JsonUtil.OBJECT_MAPPER.readValue(payload, Map.class);
            return addPayloadAndIdToParams(paramsWithBasicAuth, mapPayload, id);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    static Map<String, Object> addPayloadAndIdToParams(Map<String, Object> params, Object payload, String id) {
        return Util.merge(params, Util.map("payload", payload, "id", id));
    }

    private static void insertDocuments() throws JsonProcessingException {
        Map<String, Object> params = createDefaultProcedureParametersWithPayloadAndId(
                "{\"procedurePackage\":\"es\",\"procedureName\":\"get\",\"procedureDescription\":\"perform a GET operation to ElasticSearch\"}",
                UUID.randomUUID().toString());
        TestUtil.testCall(
                db,
                "CALL apoc.es.put($host,$index,$type,$id,'refresh=true',$payload, $config) yield value",
                params,
                r -> {
                    Object created = extractValueFromResponse(r, "$.result");
                    assertEquals("created", created);
                });

        params = createDefaultProcedureParametersWithPayloadAndId(
                "{\"procedurePackage\":\"es\",\"procedureName\":\"post\",\"procedureDescription\":\"perform a POST operation to ElasticSearch\"}",
                UUID.randomUUID().toString());
        TestUtil.testCall(
                db,
                "CALL apoc.es.put($host,$index,$type,$id,'refresh=true',$payload, $config) yield value",
                params,
                r -> {
                    Object created = extractValueFromResponse(r, "$.result");
                    assertEquals("created", created);
                });

        params = createDefaultProcedureParametersWithPayloadAndId(DOCUMENT, ES_ID);
        TestUtil.testCall(
                db,
                "CALL apoc.es.put($host,$index,$type,$id,'refresh=true',$payload, $config) yield value",
                params,
                r -> {
                    Object created = extractValueFromResponse(r, "$.result");
                    assertEquals("created", created);
                });
    }

    static Object extractValueFromResponse(Map response, String jsonPath) {
        Object jsonResponse = response.get("value");
        assertNotNull(jsonResponse);

        String json = JsonPath.parse(jsonResponse).jsonString();
        return JsonPath.parse(json, JSON_PATH_CONFIG).read(jsonPath);
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
                "CALL apoc.es.get($host,$index,$type,$id,null,null,$config) yield value",
                paramsWithBasicAuth,
                commonEsGetConsumer());
    }

    @Test
    public void testProceduresWithUrlAndHeaders() {
        TestUtil.testCall(db, "CALL apoc.es.stats($url, $config)", paramsWithBasicAuth, commonEsStatsConsumer());

        TestUtil.testCall(
                db,
                "CALL apoc.es.get($url,$index,$type,$id,null,null,$config) yield value",
                paramsWithBasicAuth,
                commonEsGetConsumer());
    }

    @Test
    public void testGetRowProcedureWithAuthHeader() {
        Map<String, Object> params = Map.of(
                "url",
                elastic.getHttpHostAddress(),
                "suffix",
                getRawProcedureUrl(ES_ID, getEsType()),
                "headers",
                basicAuthHeader);

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
                "CALL apoc.es.get('customKey',$index,$type,$id,null,null, $config) yield value",
                paramsWithBasicAuth,
                commonEsGetConsumer());

        apocConfig().getConfig().clearProperty(URL_CONF);
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
                "CALL apoc.es.get($host,$index,$type,$id,{_source_includes:'name'},null, $config) yield value",
                paramsWithBasicAuth,
                commonEsGetConsumer());
    }

    /**
     * Simple get request for document retrieval, but we also send multiple commands (as a string) to ES
     * http://localhost:9200/test-index/test-type/4fa40c40-db89-4761-b6a3-75f0015db059?_source_includes=name&_source_excludes=description
     *
     * @throws Exception
     */
    @Test
    public void testGetWithQueryAsStringMultipleParams() throws Exception {
        TestUtil.testCall(
                db,
                "CALL apoc.es.get($host,$index,$type,$id,'_source_includes=name&_source_excludes=description',null, $config) yield value",
                paramsWithBasicAuth,
                commonEsGetConsumer());
    }

    /**
     * Simple get request for document retrieval but we also send a single command (as a string) to ES
     * http://localhost:9200/test-index/test-type/4fa40c40-db89-4761-b6a3-75f0015db059?_source_includes=name
     *
     * @throws Exception
     */
    @Test
    public void testGetWithHeaderAndQueryAsStringSingleParam() throws Exception {
        TestUtil.testCall(
                db,
                "CALL apoc.es.get($host,$index,$type,$id,'_source_includes=name',null, $config) yield value",
                paramsWithBasicAuth,
                commonEsGetConsumer());
    }

    /**
     * We want to search our document by name --> /test-index/test-type/_search?
     * This test uses a plain string to query ES
     */
    @Test
    public void testSearchWithQueryNull() throws Exception {
        TestUtil.testCall(
                db, "CALL apoc.es.query($host,$index,$type,null,null, $config) yield value", paramsWithBasicAuth, r -> {
                    Object hits = extractValueFromResponse(r, "$.hits.hits");
                    assertEquals(3, ((List) hits).size());
                });
    }

    @Test
    public void testStatsWithAuthHeader() {
        TestUtil.testCall(db, "CALL apoc.es.stats($host, $config)", paramsWithBasicAuth, commonEsStatsConsumer());
    }

    @Test
    public void testSearchWithQueryAsAStringAndHeader() throws Exception {
        TestUtil.testCall(
                db,
                "CALL apoc.es.query($host, $index, $type, 'q=name:Neo4j', null, $config) yield value",
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
                db,
                "CALL apoc.es.query($host,$index,$type,'q=name:*',null, $config) yield value",
                paramsWithBasicAuth,
                r -> {
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
                "CALL apoc.es.query($host,$index,$type,'q=procedureName:get',null, $config) yield value",
                paramsWithBasicAuth,
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
                "CALL apoc.es.query($host,$index,$type,'size=1&scroll=1m&_source=true&q=procedureName:get',null, $config) yield value",
                paramsWithBasicAuth,
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
                db,
                "CALL apoc.es.put($host,$index,$type,$id,'refresh=true',$payload, $config) yield value",
                params,
                r -> {
                    Object updated = extractValueFromResponse(r, "$.result");
                    assertEquals("updated", updated);
                });

        TestUtil.testCall(db, "CALL apoc.es.get($host,$index,$type,$id,null,null, $config) yield value", params, r -> {
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
                "CALL apoc.es.put($host,$index,$type,$id,'refresh=true',$payload, $config) yield value",
                params,
                r -> {
                    Object result = extractValueFromResponse(r, "$.result");
                    assertEquals("updated", result);
                });

        TestUtil.testCall(
                db, "CALL apoc.es.get($host, $index, $type, $id, null, null, $config) yield value", params, r -> {
                    Object actualTags = extractValueFromResponse(r, "$._source.tags[0]");
                    assertEquals(tags, actualTags);
                });
    }

    @Test
    public void testPostRawCreateDocument() throws IOException {
        String index = UUID.randomUUID().toString();
        String type = getEsType();
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
        String type = getEsType();
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
                "config",
                map("headers", basicAuthHeader));

        AtomicReference<String> id = new AtomicReference<>();
        TestUtil.testCall(
                db,
                "CALL apoc.es.post($host,$index,$type,'refresh=true', $payload, $config) yield value",
                params,
                r -> {
                    Object result = extractValueFromResponse(r, "$.result");
                    assertEquals("created", result);

                    id.set((String) ((Map) r.get("value")).get("_id"));
                });

        params.put("id", id.get());

        TestUtil.testCall(
                db, "CALL apoc.es.get($host, $index, $type, $id, null, null, $config) yield value", params, r -> {
                    Object actual = extractValueFromResponse(r, "$._source.ajeje");
                    assertEquals("Brazorf", actual);
                });

        TestUtil.testCall(db, "CALL apoc.es.delete($host, $index, $type, $id, 'refresh=true', $config)", params, r -> {
            Object result = extractValueFromResponse(r, "$.result");
            assertEquals("deleted", result);
        });
    }

    /**
     * We want to search our document by name --> /test-index/test-type/_search?q=name:Neo4j
     * This test uses a Map to query ES
     */
    @Test
    public void testSearchWithQueryAsAMap() {
        TestUtil.testCall(
                db,
                "CALL apoc.es.query($host,$index,$type,null,{query: {match: {name: 'Neo4j'}}}, $config) yield value",
                paramsWithBasicAuth,
                r -> {
                    Object name = extractValueFromResponse(r, "$.hits.hits[0]._source.name");
                    assertEquals("Neo4j", name);
                });
    }

    static Consumer<Map<String, Object>> commonEsGetConsumer() {
        return r -> {
            Object name = extractValueFromResponse(r, "$._source.name");
            assertEquals("Neo4j", name);
        };
    }

    static Consumer<Map<String, Object>> commonEsStatsConsumer() {
        return commonEsStatsConsumer(3);
    }

    static Consumer<Map<String, Object>> commonEsStatsConsumer(int expectedNumOfDocs) {
        return r -> {
            assertNotNull(r.get("value"));

            Object numOfDocs = extractValueFromResponse(r, "$._all.total.docs.count");
            assertEquals(expectedNumOfDocs, numOfDocs);
        };
    }
}
