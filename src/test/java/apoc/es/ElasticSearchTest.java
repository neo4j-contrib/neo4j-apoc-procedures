package apoc.es;

import apoc.periodic.Periodic;
import apoc.util.TestUtil;
import apoc.util.Util;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.net.ConnectException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.*;

/**
 * @author mh
 * @since 21.05.16
 */
public class ElasticSearchTest {

    private final static String ES_INDEX = "test-index";

    private final static String ES_TYPE = "test-type";

    private final static String ES_ID = UUID.randomUUID().toString();

    private final static String HOST = "localhost";

    protected static GraphDatabaseService db;

    private static Map<String, Object> defaultParams = Util.map("index", ES_INDEX, "type", ES_TYPE, "id", ES_ID, "host", HOST);

    // We need a reference to the class implementing the procedures
    private final ElasticSearch es = new ElasticSearch();
    private Configuration JSON_PATH_CONFIG = Configuration.builder().options(Option.DEFAULT_PATH_LEAF_TO_NULL, Option.SUPPRESS_EXCEPTIONS).build();

    @BeforeClass
    public static void setUp() throws Exception {
        db = new TestGraphDatabaseFactory()
                .newImpermanentDatabaseBuilder()
                .newGraphDatabase();
        TestUtil.registerProcedure(db, ElasticSearch.class);
        TestUtil.registerProcedure(db, Periodic.class);

        insertDocuments();
    }

    @AfterClass
    public static void tearDown() {
        db.shutdown();
    }

    /**
     * Default params (host, index, type, id) + payload
     *
     * @param payload
     * @return
     */
    private static Map<String, Object> createDefaultProcedureParametersWithPayloadAndId(String payload, String id) {
        return Util.merge(defaultParams, Util.map("payload", payload, "id", id));
    }

    private static void insertDocuments() {
        TestUtil.ignoreException(() -> {
            Map<String, Object> params = createDefaultProcedureParametersWithPayloadAndId("{\"procedurePackage\":\"es\",\"procedureName\":\"get\",\"procedureDescription\":\"perform a GET operation to ElasticSearch\"}", UUID.randomUUID().toString());

            TestUtil.testCall(db, "CALL apoc.es.post({host},{index},{type},{id},null,{payload}) yield value", params, r -> assertNotNull(r.get("value")));
        }, ConnectException.class);

        TestUtil.ignoreException(() -> {
            Map<String, Object> params = createDefaultProcedureParametersWithPayloadAndId("{\"procedurePackage\":\"es\",\"procedureName\":\"post\",\"procedureDescription\":\"perform a POST operation to ElasticSearch\"}", UUID.randomUUID().toString());

            TestUtil.testCall(db, "CALL apoc.es.post({host},{index},{type},{id},null,{payload}) yield value", params, r -> assertNotNull(r.get("value")));
        }, ConnectException.class);

        TestUtil.ignoreException(() -> {
            Map<String, Object> params = createDefaultProcedureParametersWithPayloadAndId("{\"name\":\"Neo4j\",\"company\":\"Neo Technology\",\"description\":\"Awesome stuff with a graph database\"}", ES_ID);

            TestUtil.testCall(db, "CALL apoc.es.post({host},{index},{type},{id},null,{payload}) yield value", params, r -> assertNotNull(r.get("value")));
        }, ConnectException.class);
    }

    private final Object extractValueFromResponse(Map response, String jsonPath) {
        Object jsonResponse = response.get("value");
        assertNotNull(jsonResponse);

        String json = JsonPath.parse(jsonResponse).jsonString();
        Object value = JsonPath.parse(json, JSON_PATH_CONFIG).read(jsonPath);

        return value;
    }

    @Test
    public void testStats() throws Exception {
        TestUtil.ignoreException(() -> {
            TestUtil.testCall(db, "CALL apoc.es.stats(null)", r -> {
                assertNotNull(r.get("value"));

                Object numOfDocs = extractValueFromResponse(r, "$._all.total.docs.count");
                assertNotEquals(0, numOfDocs);
            });
        }, ConnectException.class);
    }

    /**
     * Simple get request for document retrieval
     * http://localhost:9200/test-index/test-type/ee6749ff-b836-4529-88e9-3105675d625a
     *
     * @throws Exception
     */
    @Test
    public void testGetWithQueryNull() throws Exception {
        TestUtil.ignoreException(() -> {
            TestUtil.testCall(db, "CALL apoc.es.get({host},{index},{type},{id},null,null) yield value", defaultParams, r -> {
                Object name = extractValueFromResponse(r, "$._source.name");
                assertEquals("Neo4j", name);
            });
        }, ConnectException.class);
    }

    /**
     * Simple get request for document retrieval but we also send multiple commands (as a Map) to ES
     * http://localhost:9200/test-index/test-type/4fa40c40-db89-4761-b6a3-75f0015db059?_source_include=name&_source_exclude=description
     *
     * @throws Exception
     */
    @Test
    public void testGetWithQueryAsMapMultipleParams() throws Exception {
        TestUtil.ignoreException(() -> {
            TestUtil.testCall(db, "CALL apoc.es.get({host},{index},{type},{id},{_source_include:'name',_source_exclude:'description'},null) yield value", defaultParams, r -> {
                Object name = extractValueFromResponse(r, "$._source.name");
                assertEquals("Neo4j", name);
            });
        }, ConnectException.class);
    }

    /**
     * Simple get request for document retrieval but we also send a single command (as a Map) to ES
     * http://localhost:9200/test-index/test-type/4fa40c40-db89-4761-b6a3-75f0015db059?_source_include=name
     *
     * @throws Exception
     */
    @Test
    public void testGetWithQueryAsMapSingleParam() throws Exception {
        TestUtil.ignoreException(() -> {
            TestUtil.testCall(db, "CALL apoc.es.get({host},{index},{type},{id},{_source_include:'name'},null) yield value", defaultParams, r -> {
                Object name = extractValueFromResponse(r, "$._source.name");
                assertEquals("Neo4j", name);
            });
        }, ConnectException.class);
    }

    /**
     * Simple get request for document retrieval but we also send multiple commands (as a string) to ES
     * http://localhost:9200/test-index/test-type/4fa40c40-db89-4761-b6a3-75f0015db059?_source_include=name&_source_exclude=description
     *
     * @throws Exception
     */
    @Test
    public void testGetWithQueryAsStringMultipleParams() throws Exception {
        TestUtil.ignoreException(() -> {
            TestUtil.testCall(db, "CALL apoc.es.get({host},{index},{type},{id},'_source_include=name&_source_exclude=description',null) yield value", defaultParams, r -> {
                Object name = extractValueFromResponse(r, "$._source.name");
                assertEquals("Neo4j", name);
            });
        }, ConnectException.class);
    }

    /**
     * Simple get request for document retrieval but we also send a single command (as a string) to ES
     * http://localhost:9200/test-index/test-type/4fa40c40-db89-4761-b6a3-75f0015db059?_source_include=name
     *
     * @throws Exception
     */
    @Test
    public void testGetWithQueryAsStringSingleParam() throws Exception {
        TestUtil.ignoreException(() -> {
            TestUtil.testCall(db, "CALL apoc.es.get({host},{index},{type},{id},'_source_include=name',null) yield value", defaultParams, r -> {
                Object name = extractValueFromResponse(r, "$._source.name");
                assertEquals("Neo4j", name);
            });
        }, ConnectException.class);
    }

    /**
     * We want to search our document by name --> /test-index/test-type/_search?q=name:Neo4j
     * This test uses a plain string to query ES
     */
    @Test
    public void testSearchWithQueryAsAString() throws Exception {
        TestUtil.ignoreException(() -> {
            TestUtil.testCall(db, "CALL apoc.es.query({host},{index},{type},'name:Neo4j',null) yield value", defaultParams, r -> {
                Object name = extractValueFromResponse(r, "$.hits.hits[0]._source.name");
                assertEquals("Neo4j", name);
            });
        }, ConnectException.class);
    }

    /**
     * We want to add a field to an existing document posting a request with a payload
     * http://localhost:9200/test-index/test-type/0b727048-a6ca-44f4-906b-f0e86ed65c7e + payload: {"event":"Graph Connect 2017"}
     */
    @Test
    public void testPostAddNewDocument() {
        TestUtil.ignoreException(() -> {
            Map<String, Object> params = createDefaultProcedureParametersWithPayloadAndId("{\"event\":\"Graph Connect 2017\"}", UUID.randomUUID().toString());

            TestUtil.testCall(db, "CALL apoc.es.post({host},{index},{type},{id},null,{payload}) yield value", params, r -> {
                Object response = extractValueFromResponse(r, "$._shards.successful");
                assertEquals(1, response);
            });

            // We try to get the document back
            TestUtil.testCall(db, "CALL apoc.es.get({host},{index},{type},{id},null,null) yield value", params, r -> {
                Object event = extractValueFromResponse(r, "$._source.event");
                assertEquals("Graph Connect 2017", event);
            });
        }, ConnectException.class);
    }

    /**
     * We create a document with a field tags that is a collection of a single element "awesome".
     * Then we update the same field with the collection ["beautiful"]
     * and we retrieve the document in order to verify the update.
     * <p>
     * http://localhost:9200/test-index/test-type/f561c1c5-4092-4c5d-98a6-5ea2b3417415/_update
     */
    @Test
    public void testPostUpdateDocument() {
        TestUtil.ignoreException(() -> {
            String id = UUID.randomUUID().toString();
            Map<String, Object> params = createDefaultProcedureParametersWithPayloadAndId("{\"tags\":[\"awesome\"]}", id);

            TestUtil.testCall(db, "CALL apoc.es.put({host},{index},{type},{id},null,{payload}) yield value", params, r -> {
                Object created = extractValueFromResponse(r, "$.created");
                assertEquals(true, created);
            });

            params = createDefaultProcedureParametersWithPayloadAndId("{\"doc\":{\"tags\":[\"beautiful\"]}}", id + "/_update");
            TestUtil.testCall(db, "CALL apoc.es.post({host},{index},{type},{id},null,{payload}) yield value", params, r -> {
                Object updated = extractValueFromResponse(r, "$.result");
                assertEquals("updated", updated);
            });

            // We try to get the document back
            params = createDefaultProcedureParametersWithPayloadAndId("", id);
            TestUtil.testCall(db, "CALL apoc.es.get({host},{index},{type},{id},null,null) yield value", params, r -> {
                Object tag = extractValueFromResponse(r, "$._source.tags[0]");
                assertEquals("beautiful", tag);
            });
        }, ConnectException.class);
    }

    /**
     * We create a new document and retrieve it to check the its field
     * http://localhost:9200/test-index/test-type/e360f95f-490c-4713-b343-db1c4a5d7dcf
     */
    @Test
    public void testPutNewDocument() {
        TestUtil.ignoreException(() -> {
            Map<String, Object> params = createDefaultProcedureParametersWithPayloadAndId("{\"tags\":[\"awesome\"]}", UUID.randomUUID().toString());

            TestUtil.testCall(db, "CALL apoc.es.put({host},{index},{type},{id},null,{payload}) yield value", params, r -> {
                Object created = extractValueFromResponse(r, "$.created");
                assertEquals(true, created);
            });

            // We try to get the document back
            TestUtil.testCall(db, "CALL apoc.es.get({host},{index},{type},{id},null,null) yield value", params, r -> {
                Object tag = extractValueFromResponse(r, "$._source.tags[0]");
                assertEquals("awesome", tag);
            });
        }, ConnectException.class);
    }

    /**
     * We want to to search our document by name --> /test-index/test-type/_search?q=name:Neo4j
     * This test uses a Map to query ES
     */
    @Test
    public void testSearchWithQueryAsAMap() {
        TestUtil.ignoreException(() -> {
            TestUtil.testCall(db, "CALL apoc.es.query('" + HOST + "','" + ES_INDEX + "','" + ES_TYPE + "',{name:'Neo4j'},null) yield value", r -> {
                Object name = extractValueFromResponse(r, "$.hits.hits[0]._source.name");
                assertEquals("Neo4j", name);
            });
        }, ConnectException.class);
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

        String queryUrl = hostUrl + String.format("/%s/%s/%s?%s", index == null ? "_all" : index,
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
        String queryUrl = hostUrl + String.format("/%s/%s/%s?%s", index == null ? "_all" : index,
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
        String queryUrl = hostUrl + String.format("/%s/%s/%s?%s", index == null ? "_all" : index,
                type == null ? "_all" : type,
                id == null ? "" : id,
                es.toQueryParams(new HashMap<String, String>()));

        // First we test the older version against the newest one
        assertNotEquals(queryUrl, es.getQueryUrl(host, index, type, id, new HashMap<String, String>()));
        assertTrue(!es.getQueryUrl(host, index, type, id, new HashMap<String, String>()).endsWith("?"));
    }
}