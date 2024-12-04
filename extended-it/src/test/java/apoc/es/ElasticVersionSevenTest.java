package apoc.es;

import apoc.util.TestUtil;
import apoc.util.UtilExtended;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static apoc.ApocConfig.apocConfig;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class ElasticVersionSevenTest extends ElasticSearchTest {

    public static final String ES_TYPE = UUID.randomUUID().toString();
    private final static String HOST = "localhost";
    public static final ElasticSearchHandler DEFAULT_HANDLER = ElasticSearchHandler.Version.DEFAULT.get();

    private static final Map<String, Object> defaultParams = UtilExtended.map("index", ES_INDEX, "type", ES_TYPE, "id", ES_ID);
    
    @BeforeClass
    public static void setUp() throws Exception {
        
        Map<String, Object> config = Map.of("headers", basicAuthHeader);
        Map<String, Object> params = new HashMap<>(defaultParams);
        params.put("config", config);


        String tag = "7.9.2";
        getElasticContainer(tag, Map.of(), params);
        
        String httpHostAddress = elastic.getHttpHostAddress();
        HTTP_HOST_ADDRESS = String.format("elastic:%s@%s",
                password,
                httpHostAddress);

        HTTP_URL_ADDRESS = "http://" + HTTP_HOST_ADDRESS;

        defaultParams.put("host", HTTP_HOST_ADDRESS);
        defaultParams.put("url", HTTP_URL_ADDRESS);
    }

    @Override
    String getEsType() {
        return ES_TYPE;
    }
    
    
    /*
    Tests without basic auth header
    */

    @Test
    public void testGetRowProcedure() {
        Map<String, Object> params = Map.of("url", HTTP_URL_ADDRESS, "suffix", getRawProcedureUrl(ES_ID, ES_TYPE));

        TestUtil.testCall(db, "CALL apoc.es.getRaw($url,$suffix, null)", params,
                commonEsGetConsumer());
    }

    @Test
    public void testStats() throws Exception {
        TestUtil.testCall(db, "CALL apoc.es.stats($host)", defaultParams, 
                commonEsStatsConsumer());

    }

    @Test
    public void testProceduresWithUrl() {
        TestUtil.testCall(db, "CALL apoc.es.stats($url)", defaultParams, 
                commonEsStatsConsumer());

        TestUtil.testCall(db, "CALL apoc.es.get($url,$index,$type,$id,null,null) yield value", defaultParams, 
                commonEsGetConsumer());
    }

    @Test
    public void testProceduresWithUrlKeyConf() {
        apocConfig().setProperty("apoc.es.myUrlKey.url", HTTP_URL_ADDRESS);

        TestUtil.testCall(db, "CALL apoc.es.stats('myUrlKey')", 
                commonEsStatsConsumer());

        TestUtil.testCall(db, "CALL apoc.es.get('myUrlKey',$index,$type,$id,null,null, $config) yield value", paramsWithBasicAuth, 
                commonEsGetConsumer());
    }

    @Test
    public void testProceduresWithHostKeyConf() {
        apocConfig().setProperty("apoc.es.myHostKey.host", HTTP_HOST_ADDRESS);

        TestUtil.testCall(db, "CALL apoc.es.stats('myHostKey')",
                commonEsStatsConsumer());

        TestUtil.testCall(db, "CALL apoc.es.get('myHostKey',$index,$type,$id,null,null, $config) yield value", paramsWithBasicAuth,
                commonEsGetConsumer());
    }
    
    @Test
    public void testGetWithQueryAsStringSingleParam() {
        TestUtil.testCall(db, "CALL apoc.es.get($host,$index,$type,$id,'_source_includes=name',null, {}) yield value", defaultParams,
                commonEsGetConsumer());
    }

    /**
     * We want to search our document by name --> /test-index/test-type/_search?q=name:Neo4j
     * This test uses a plain string to query ES
     */
    @Test
    public void testSearchWithQueryAsAString() {
        TestUtil.testCall(db, "CALL apoc.es.query($host,$index,$type,'q=name:Neo4j',null) yield value", defaultParams, r -> {
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
        String hostUrl = DEFAULT_HANDLER.getElasticSearchUrl(host);

        String queryUrl = hostUrl + String.format("/%s/%s/%s?%s", index == null ? "_all" : index,
                type == null ? "_all" : type,
                id == null ? "" : id,
                DEFAULT_HANDLER.toQueryParams(query));

        assertEquals(queryUrl, DEFAULT_HANDLER.getQueryUrl(host, index, type, id, query));
    }

    @Test
    public void testGetQueryUrlShouldNotHaveTrailingQuestionMarkIfQueryIsNull() {
        String index = ES_INDEX;
        String type = ES_TYPE;
        String id = ES_TYPE;

        String host = HOST;
        String hostUrl = DEFAULT_HANDLER.getElasticSearchUrl(host);
        String queryUrl = hostUrl + String.format("/%s/%s/%s?%s", index,
                type == null ? "_all" : type,
                id == null ? "" : id,
                DEFAULT_HANDLER.toQueryParams(null));

        // First we test the older version against the newest one
        assertNotEquals(queryUrl, DEFAULT_HANDLER.getQueryUrl(host, index, type, id, null));
        assertFalse(DEFAULT_HANDLER.getQueryUrl(host, index, type, id, null).endsWith("?"));
    }

    @Test
    public void testGetQueryUrlShouldNotHaveTrailingQuestionMarkIfQueryIsEmpty() {
        String index = ES_INDEX;
        String type = ES_TYPE;
        String id = ES_ID;

        String host = HOST;
        String hostUrl = DEFAULT_HANDLER.getElasticSearchUrl(host);
        String queryUrl = hostUrl + String.format("/%s/%s/%s?%s", index == null ? "_all" : index,
                type == null ? "_all" : type,
                id == null ? "" : id,
                DEFAULT_HANDLER.toQueryParams(new HashMap<String, String>()));

        // First we test the older version against the newest one
        assertNotEquals(queryUrl, DEFAULT_HANDLER.getQueryUrl(host, index, type, id, new HashMap<String, String>()));
        assertTrue(!DEFAULT_HANDLER.getQueryUrl(host, index, type, id, new HashMap<String, String>()).endsWith("?"));
    }

    /**
     * Simple get request for document retrieval but we also send multiple commands (as a Map) to ES
     * http://localhost:9200/test-index/test-type/4fa40c40-db89-4761-b6a3-75f0015db059?_source_includes=name&_source_excludes=description
     *
     * @throws Exception
     */
    @Test
    public void testGetWithQueryAsMapMultipleParams() throws Exception {
        TestUtil.testCall(db, "CALL apoc.es.get($host,$index,$type,$id,{_source_includes:'name',_source_excludes:'description'},null) yield value", defaultParams,
                commonEsGetConsumer());
    }

}
