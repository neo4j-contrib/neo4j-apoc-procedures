package apoc.full.it.vectordb;

import apoc.util.MapUtil;
import apoc.util.Neo4jContainerExtension;
import apoc.util.TestContainerUtil;
import apoc.util.WeaviateTestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.neo4j.driver.Session;
import org.testcontainers.containers.Network;

import java.util.List;
import java.util.Map;

import static apoc.ml.RestAPIConfig.HEADERS_KEY;
import static apoc.util.TestContainerUtil.createEnterpriseDB;
import static apoc.util.TestContainerUtil.testCall;
import static apoc.util.TestContainerUtil.testCallEmpty;
import static apoc.util.TestContainerUtil.testResult;
import static apoc.util.Util.map;
import static apoc.util.WeaviateTestUtil.ADMIN_AUTHORIZATION;
import static apoc.util.WeaviateTestUtil.ADMIN_HEADER_CONF;
import static apoc.util.WeaviateTestUtil.COLLECTION_NAME;
import static apoc.util.WeaviateTestUtil.FIELDS;
import static apoc.util.WeaviateTestUtil.HOST;
import static apoc.util.WeaviateTestUtil.ID_1;
import static apoc.util.WeaviateTestUtil.ID_2;
import static apoc.util.WeaviateTestUtil.WEAVIATE_CONTAINER;
import static apoc.util.WeaviateTestUtil.WEAVIATE_CREATE_COLLECTION_APOC;
import static apoc.util.WeaviateTestUtil.WEAVIATE_DELETE_COLLECTION_APOC;
import static apoc.util.WeaviateTestUtil.WEAVIATE_DELETE_VECTOR_APOC;
import static apoc.util.WeaviateTestUtil.WEAVIATE_PORT;
import static apoc.util.WeaviateTestUtil.WEAVIATE_QUERY_APOC;
import static apoc.util.WeaviateTestUtil.WEAVIATE_UPSERT_QUERY;
import static apoc.vectordb.VectorEmbeddingConfig.ALL_RESULTS_KEY;
import static apoc.vectordb.VectorEmbeddingConfig.FIELDS_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;


public class WeaviateEnterpriseTest {

    @ClassRule
    public static TemporaryFolder storeDir = new TemporaryFolder();
    private static Neo4jContainerExtension neo4jContainer;
    private static Session session;

    @BeforeClass
    public static void setUp() throws Exception {
        Network network = Network.newNetwork();
        
        // We build the project, the artifact will be placed into ./build/libs
        neo4jContainer = createEnterpriseDB(List.of(TestContainerUtil.ApocPackage.EXTENDED), true)
                .withNetwork(network)
                .withNetworkAliases("neo4j");
        neo4jContainer.start();
        session = neo4jContainer.getSession();

        String weaviateAlias = "weaviate";
        WEAVIATE_CONTAINER.withNetwork(network)
                .withNetworkAliases(weaviateAlias)
                .withExposedPorts(WEAVIATE_PORT);
        WEAVIATE_CONTAINER.start();
        
        HOST = weaviateAlias + ":" + WEAVIATE_PORT;

        testCall(session, WEAVIATE_CREATE_COLLECTION_APOC,
                map("host", HOST, "conf", ADMIN_HEADER_CONF),
                r -> {
                    Map value = (Map) r.get("value");
                    assertEquals("TestCollection", value.get("class"));
                });

        testResult(session, WEAVIATE_UPSERT_QUERY,
                map("host", HOST, "id1", ID_1, "id2", ID_2, "conf", ADMIN_HEADER_CONF),
                r -> {
                    Map<String, Object> row = r.next();
                    Map<String, Object> value = (Map<String, Object>) row.get("value");

                    assertEquals(COLLECTION_NAME, value.get("class"));

                    while (r.hasNext()) {
                        row = r.next();
                        value = (Map<String, Object>) row.get("value");
                        assertEquals(COLLECTION_NAME, value.get("class"));
                    }
                    assertFalse(r.hasNext());
                });

        // -- delete vector
        testCall(session, WEAVIATE_DELETE_VECTOR_APOC,
                map("host", HOST, "conf", ADMIN_HEADER_CONF),
                r -> {
                    List value = (List) r.get("value");
                    assertEquals(List.of("7ef2b3a7-1e56-4ddd-b8c3-2ca8901ce308", "7ef2b3a7-1e56-4ddd-b8c3-2ca8901ce309"), value);
                });
    }

    @AfterClass
    public static void tearDown() throws Exception {
        testCallEmpty(session, WEAVIATE_DELETE_COLLECTION_APOC,
                MapUtil.map("host", HOST, "collectionName", COLLECTION_NAME, "conf", ADMIN_HEADER_CONF)
        );
        session.close();
        neo4jContainer.close();
        WEAVIATE_CONTAINER.stop();
    }

    @Test
    public void queryVectors() {
        testResult(session,  WEAVIATE_QUERY_APOC,
                map("host", HOST, "conf", map(ALL_RESULTS_KEY, true, FIELDS_KEY, FIELDS, HEADERS_KEY, ADMIN_AUTHORIZATION)),
                WeaviateTestUtil::queryVectorsAssertions);
    }
}
