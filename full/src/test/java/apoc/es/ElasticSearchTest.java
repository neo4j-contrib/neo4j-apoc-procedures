package apoc.es;

import apoc.util.JsonUtil;
import apoc.util.TestUtil;
import apoc.util.Util;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;
import org.testcontainers.elasticsearch.ElasticsearchContainer;

import java.io.IOException;
import java.util.*;

import static apoc.util.TestUtil.isRunningInCI;
import static org.junit.Assert.*;
import static org.junit.Assume.*;

/**
 * @author mh
 * @since 21.05.16
 */
public class ElasticSearchTest {

    public static ElasticsearchContainer elastic;

    private final static String ES_INDEX = "test-index";

    private final static String ES_TYPE = "test-type";

    private final static String ES_ID = UUID.randomUUID().toString();

    private final static String HOST = "localhost";

    private static final String DOCUMENT = "{\"name\":\"Neo4j\",\"company\":\"Neo Technology\",\"description\":\"Awesome stuff with a graph database\"}";

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    private static Map<String, Object> defaultParams = Util.map("index", ES_INDEX, "type", ES_TYPE, "id", ES_ID);

    // We need a reference to the class implementing the procedures
    private final ElasticSearch es = new ElasticSearch();
    private static final Configuration JSON_PATH_CONFIG = Configuration.builder().options(Option.DEFAULT_PATH_LEAF_TO_NULL, Option.SUPPRESS_EXCEPTIONS).build();

    @BeforeClass
    public static void setUp() throws Exception {
        assumeFalse(isRunningInCI());
        TestUtil.ignoreException(() -> {
            elastic = new ElasticsearchContainer();
            elastic.start();
        }, Exception.class);
        assumeNotNull(elastic);
        assumeTrue("Elastic Search must be running", elastic.isRunning());
        defaultParams.put("host", elastic.getHttpHostAddress());
        TestUtil.registerProcedure(db, ElasticSearch.class);
        insertDocuments();
    }

    @AfterClass
    public static void tearDown() {
        if (elastic != null) {
            elastic.stop();
        }
    }

    /**
     * Default params (host, index, type, id) + payload
     *
     * @param payload
     * @return
     */
    private static Map<String, Object> createDefaultProcedureParametersWithPayloadAndId(String payload, String id) {
        try {
            return Util.merge(defaultParams, Util.map("payload", JsonUtil.OBJECT_MAPPER.readValue(payload, Map.class), "id", id));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void insertDocuments() {
        Map<String, Object> params = createDefaultProcedureParametersWithPayloadAndId("{\"procedurePackage\":\"es\",\"procedureName\":\"get\",\"procedureDescription\":\"perform a GET operation to ElasticSearch\"}", UUID.randomUUID().toString());
        TestUtil.testCall(db, "CALL apoc.es.put($host,$index,$type,$id,'refresh=true',$payload) yield value", params, r -> {
            Object created = extractValueFromResponse(r, "$.result");
            assertEquals("created", created);
        });

        params = createDefaultProcedureParametersWithPayloadAndId("{\"procedurePackage\":\"es\",\"procedureName\":\"post\",\"procedureDescription\":\"perform a POST operation to ElasticSearch\"}", UUID.randomUUID().toString());
        TestUtil.testCall(db, "CALL apoc.es.put($host,$index,$type,$id,'refresh=true',$payload) yield value", params, r -> {
            Object created = extractValueFromResponse(r, "$.result");
            assertEquals("created", created);
        });

        params = createDefaultProcedureParametersWithPayloadAndId(DOCUMENT, ES_ID);
        TestUtil.testCall(db, "CALL apoc.es.put($host,$index,$type,$id,'refresh=true',$payload) yield value", params, r -> {
            Object created = extractValueFromResponse(r, "$.result");
            assertEquals("created", created);
        });
    }

    private static final Object extractValueFromResponse(Map response, String jsonPath) {
        Object jsonResponse = response.get("value");
        assertNotNull(jsonResponse);

        String json = JsonPath.parse(jsonResponse).jsonString();
        Object value = JsonPath.parse(json, JSON_PATH_CONFIG).read(jsonPath);

        return value;
    }

    @Test
    public void testStats() throws Exception {
        TestUtil.testCall(db, "CALL apoc.es.stats($host)", defaultParams, r -> {
            assertNotNull(r.get("value"));

            Object numOfDocs = extractValueFromResponse(r, "$._all.total.docs.count");
            assertNotEquals(0, numOfDocs);
        });
    }

    /**
     * Simple get request for document retrieval
     * http://localhost:9200/test-index/test-type/ee6749ff-b836-4529-88e9-3105675d625a
     *
     * @throws Exception
     */
    @Test
    public void testGetWithQueryNull() throws Exception {
        TestUtil.testCall(db, "CALL apoc.es.get($host,$index,$type,$id,null,null) yield value", defaultParams, r -> {
            Object name = extractValueFromResponse(r, "$._source.name");
            assertEquals("Neo4j", name);
        });
    }

    /**
     * Simple get request for document retrieval but we also send multiple commands (as a Map) to ES
     * http://localhost:9200/test-index/test-type/4fa40c40-db89-4761-b6a3-75f0015db059?_source_includes=name&_source_excludes=description
     *
     * @throws Exception
     */
    @Test
    public void testGetWithQueryAsMapMultipleParams() throws Exception {
        TestUtil.testCall(db, "CALL apoc.es.get($host,$index,$type,$id,{_source_includes:'name',_source_excludes:'description'},null) yield value", defaultParams, r -> {
            Object name = extractValueFromResponse(r, "$._source.name");
            assertEquals("Neo4j", name);
        });
    }

    /**
     * Simple get request for document retrieval but we also send a single command (as a Map) to ES
     * http://localhost:9200/test-index/test-type/4fa40c40-db89-4761-b6a3-75f0015db059?_source_includes=name
     *
     * @throws Exception
     */
    @Test
    public void testGetWithQueryAsMapSingleParam() throws Exception {
        TestUtil.testCall(db, "CALL apoc.es.get($host,$index,$type,$id,{_source_includes:'name'},null) yield value", defaultParams, r -> {
            Object name = extractValueFromResponse(r, "$._source.name");
            assertEquals("Neo4j", name);
        });
    }

    /**
     * Simple get request for document retrieval but we also send multiple commands (as a string) to ES
     * http://localhost:9200/test-index/test-type/4fa40c40-db89-4761-b6a3-75f0015db059?_source_includes=name&_source_excludes=description
     *
     * @throws Exception
     */
    @Test
    public void testGetWithQueryAsStringMultipleParams() throws Exception {
        TestUtil.testCall(db, "CALL apoc.es.get($host,$index,$type,$id,'_source_includes=name&_source_excludes=description',null) yield value", defaultParams, r -> {
            Object name = extractValueFromResponse(r, "$._source.name");
            assertEquals("Neo4j", name);
        });
    }

    /**
     * Simple get request for document retrieval but we also send a single command (as a string) to ES
     * http://localhost:9200/test-index/test-type/4fa40c40-db89-4761-b6a3-75f0015db059?_source_includes=name
     *
     * @throws Exception
     */
    @Test
    public void testGetWithQueryAsStringSingleParam() throws Exception {
        TestUtil.testCall(db, "CALL apoc.es.get($host,$index,$type,$id,'_source_includes=name',null) yield value", defaultParams, r -> {
            Object name = extractValueFromResponse(r, "$._source.name");
            assertEquals("Neo4j", name);
        });
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
        TestUtil.testCall(db, "CALL apoc.es.query($host,$index,$type,'q=name:Neo4j',null) yield value", defaultParams, r -> {
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
        TestUtil.testCall(db, "CALL apoc.es.query($host,$index,$type,'q=name:*',null) yield value", defaultParams, r -> {
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
        TestUtil.testCall(db, "CALL apoc.es.query($host,$index,$type,'q=procedureName:get',null) yield value", defaultParams, r -> {
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
        TestUtil.testCall(db, "CALL apoc.es.query($host,$index,$type,'size=1&scroll=1m&_source=true&q=procedureName:get',null) yield value", defaultParams, r -> {
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
    public void testPostUpdateDocument() throws IOException{
        Map<String, Object> doc = JsonUtil.OBJECT_MAPPER.readValue(DOCUMENT, Map.class);
        doc.put("tags", Arrays.asList("awesome"));
        Map<String, Object> params = createDefaultProcedureParametersWithPayloadAndId(JsonUtil.OBJECT_MAPPER.writeValueAsString(doc), ES_ID);
        TestUtil.testCall(db, "CALL apoc.es.put($host,$index,$type,$id,'refresh=true',$payload) yield value", params, r -> {
            Object updated = extractValueFromResponse(r, "$.result");
            assertEquals("updated", updated);
        });

        TestUtil.testCall(db, "CALL apoc.es.get($host,$index,$type,$id,null,null) yield value", params, r -> {
            Object tag = extractValueFromResponse(r, "$._source.tags[0]");
            assertEquals("awesome", tag);
        });
    }

    /**
     * We want to to search our document by name --> /test-index/test-type/_search?q=name:Neo4j
     * This test uses a Map to query ES
     */
    @Test
    public void testSearchWithQueryAsAMap() {
        TestUtil.testCall(db, "CALL apoc.es.query($host,$index,$type,null,{query: {match: {name: 'Neo4j'}}}) yield value",
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
