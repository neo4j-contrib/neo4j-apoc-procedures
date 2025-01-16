package apoc.full.it.es;

import apoc.es.ElasticSearchHandler;
import apoc.util.TestUtil;
import apoc.util.Util;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static apoc.es.ElasticSearchConfig.VERSION_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ElasticVersionEightTest extends ElasticSearchTest {
    public static final String ES_TYPE = "_doc";

    @BeforeClass
    public static void setUp() throws Exception {
        Map<String, Object> config = Map.of("headers", basicAuthHeader, VERSION_KEY, ElasticSearchHandler.Version.EIGHT.name());
        Map<String, Object> params = Util.map("index", ES_INDEX,
                "id", ES_ID, "type", ES_TYPE, "config", config);

        String tag = "8.12.1";
        Map<String, String> envMap = Map.of(
                "xpack.security.http.ssl.enabled", "false",
                "cluster.routing.allocation.disk.threshold_enabled","false"
        );

        getElasticContainer(tag, envMap, params);
    }

    @Override
    String getEsType() {
        return ES_TYPE;
    }

    @Test
    public void testCreateIndexAPI() {
        TestUtil.testCall(db, "CALL apoc.es.put($host,'my-index-000001',null,null,null,null,$config)",
                paramsWithBasicAuth,
                r -> {
                    Object actual = ((Map) r.get("value")).get("index");
                    assertEquals("my-index-000001", actual);
                });
    }

    @Test
    public void testGetIndexAPI() {
        TestUtil.testCall(db, "CALL apoc.es.get($host,$index,null,null,null,null,$config) yield value",
                paramsWithBasicAuth,
                r -> {
                    Set valueKeys = ((Map) r.get("value")).keySet();
                    assertEquals(Set.of(ES_INDEX), valueKeys);
                });
    }

    @Test
    public void testSearchWithQueryAsPayload() {
        TestUtil.testCall(db, "CALL apoc.es.query($host, $index, null, 'pretty', {`_source`: {includes: ['name']}}, $config) yield value", paramsWithBasicAuth,
                this::searchQueryPayloadAssertions);
    }

    @Test
    public void testSearchWithQueryAsPayloadAndWithoutIndex() {
        TestUtil.testCall(db, "CALL apoc.es.query($host, null, null, 'pretty', {`_source`: {includes: ['name']}}, $config) yield value", paramsWithBasicAuth,
                this::searchQueryPayloadAssertions);
    }

    private void searchQueryPayloadAssertions(Map<String, Object> r) {
        List<Map> values = (List<Map>) extractValueFromResponse(r, "$.hits.hits");
        assertEquals(3, values.size());

        values.forEach(item -> {
            Map source = (Map) item.get("_source");

            assertTrue("Actual _source is: " + source,
                    source.equals(Map.of()) || source.equals(Map.of("name", "Neo4j"))
            );
        });
    }

}