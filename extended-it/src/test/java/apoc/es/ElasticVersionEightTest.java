package apoc.es;

import apoc.util.JsonUtil;
import apoc.util.TestUtil;
import apoc.util.Util;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.nimbusds.jose.util.Pair;
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
        
        String tag = "8.14.3";
        Map<String, String> envMap = Map.of(
                "xpack.security.http.ssl.enabled", "false",
                "cluster.routing.allocation.disk.threshold_enabled","false",
                "xpack.license.self_generated.type", "trial" // To avoid error "current license is non-compliant for [Reciprocal Rank Fusion (RRF)]"
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

    @Test
    public void testSearchRRF() throws JsonProcessingException {
        String payload = """
            {
              "mappings": {
                "properties": {
                  "text": {
                    "type": "text"
                  },
                  "vector": {
                    "type": "dense_vector",
                    "dims": 1,
                    "index": true,
                    "similarity": "l2_norm"
                  },
                  "integer": {
                    "type": "integer"
                  }
                }
              }
            }
            """;

        setPayload(payload, paramsWithBasicAuth);
        TestUtil.testCall(db, "CALL apoc.es.put($host, 'example-index', null, null, null, $payload, $config)",
                paramsWithBasicAuth,
                r -> {
                    Object actual = ((Map) r.get("value")).get("index");
                    assertEquals("example-index", actual);
                });

        assertPutForRRF();

        paramsWithBasicAuth.remove("payload");
        TestUtil.testCall(db, "CALL apoc.es.post($host, 'example-index/_refresh', null, null, '', $config)",
                paramsWithBasicAuth,
                r -> {
                    Object actual = ((Map) ((Map) r.get("value")).get("_shards")).get("successful");
                    assertEquals(1L, actual);
                });

        payload = """
             {
                 "retriever": {
                     "rrf": {
                         "retrievers": [
                             {
                                 "standard": {
                                     "query": {
                                         "term": {
                                             "text": "rrf"
                                         }
                                     }
                                 }
                             },
                             {
                                 "knn": {
                                     "field": "vector",
                                     "query_vector": [3],
                                     "k": 5,
                                     "num_candidates": 5
                                 }
                             }
                         ],
                         "window_size": 5,
                         "rank_constant": 1
                     }
                 },
                 "size": 3,
                 "aggs": {
                     "int_count": {
                         "terms": {
                             "field": "integer"
                         }
                     }
                 }
             }
                """;

        setPayload(payload, paramsWithBasicAuth);
        TestUtil.testCall(db, "CALL apoc.es.getRaw($host,'example-index/_search',$payload,$config) yield value",
                paramsWithBasicAuth,
                r -> {
                    Object result = ((Map) ((Map) ((Map) r.get("value")).get("hits")).get("total")).get("value");
                    assertEquals(5L, result);
                });

        TestUtil.testCall(db, "CALL apoc.es.delete($host,'example-index',null,null,null,$config)",
                paramsWithBasicAuth,
                r -> {
                    boolean acknowledged = ((boolean) ((Map) r.get("value")).get("acknowledged"));
                    assertTrue(acknowledged);
                });

        paramsWithBasicAuth.put("index", ES_INDEX);
    }

    private void assertPutForRRF() {
        List<Pair<String, String>> payloads = List.of(
                Pair.of("example-index/_doc/1", "{ \"text\" : \"rrf\", \"vector\" : [5], \"integer\": 1 }"),
                Pair.of("example-index/_doc/2", "{ \"text\" : \"rrf rrf\", \"vector\" : [4], \"integer\": 2 }"),
                Pair.of("example-index/_doc/3", "{ \"text\" : \"rrf rrf rrf\", \"vector\" : [3], \"integer\": 1 }"),
                Pair.of("example-index/_doc/4", "{ \"text\" : \"rrf rrf rrf rrf\", \"integer\": 2 }"),
                Pair.of("example-index/_doc/5", "{ \"vector\" : [0], \"integer\": 1 }")
        );

        payloads.forEach(payload -> {
            try {
                Map mapPayload = JsonUtil.OBJECT_MAPPER.readValue(payload.getRight(), Map.class);
                paramsWithBasicAuth.put("payload", mapPayload);
                paramsWithBasicAuth.put("index", payload.getLeft());
                TestUtil.testCall(db, "CALL apoc.es.put($host, $index, null, null, null, $payload, $config)",
                        paramsWithBasicAuth,
                        r -> {
                            Object actual = ((Map) r.get("value")).get("result");
                            assertEquals("created", actual);
                        });
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        });

    }

    private void setPayload(String payload, Map<String, Object> params) throws JsonProcessingException {
        Map<String, Object> mapPayload = JsonUtil.OBJECT_MAPPER.readValue(payload, Map.class);
        params.put("payload", mapPayload);
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
