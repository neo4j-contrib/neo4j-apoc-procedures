package apoc.vectordb;

import apoc.util.RecordingHttpServer;
import apoc.util.RecordingHttpServer.RecordedRequest;
import apoc.util.TestUtil;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

import java.util.List;
import java.util.Map;

import static apoc.ml.RestAPIConfig.ENDPOINT_KEY;
import static apoc.ml.RestAPIConfig.HEADERS_KEY;
import static apoc.util.ExtendedTestUtil.assertFails;
import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testCall;
import static apoc.vectordb.VectorDbHandler.Type.PINECONE;
import static apoc.vectordb.VectorDbHandler.Type.QDRANT;
import static apoc.vectordb.VectorDbUtil.ERROR_UNTRUSTED_ENDPOINT;
import static org.junit.Assert.assertEquals;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;

/**
 * Mock tests, with a localhost endpoint URL.
 * `localhost` and `127.0.0.1` reach the same mock server, but are different origins:
 * the former plays the host stored via `apoc.vectordb.configure`, the latter a caller-chosen one.
 */
public class VectorDbEndpointGuardTest {
    private static final String STORED_SECRET = "stored-secret";
    private static final String PINECONE_KEY = "org-pinecone";
    private static final String QDRANT_KEY = "org-qdrant";
    private static final String NO_HOST_KEY = "org-no-host";
    private static final String PINECONE_INFO = "CALL apoc.vectordb.pinecone.info($hostOrKey, 'demo-index', $conf)";
    private static final String QDRANT_INFO = "CALL apoc.vectordb.qdrant.info($hostOrKey, 'demo-collection', $conf)";

    @ClassRule
    public static TemporaryFolder storeDir = new TemporaryFolder();

    private static DatabaseManagementService databaseManagementService;
    private static GraphDatabaseService db;
    private static RecordingHttpServer server;
    private static String storedUrl;
    private static String callerUrl;

    @BeforeClass
    public static void setUp() throws Exception {
        server = new RecordingHttpServer("{}");
        storedUrl = "http://localhost:" + server.getPort();
        callerUrl = "http://127.0.0.1:" + server.getPort();

        databaseManagementService = new TestDatabaseManagementServiceBuilder(storeDir.getRoot().toPath())
                .build();
        db = databaseManagementService.database(DEFAULT_DATABASE_NAME);
        GraphDatabaseService sysDb = databaseManagementService.database(SYSTEM_DATABASE_NAME);
        TestUtil.registerProcedure(db, VectorDb.class, Pinecone.class, Qdrant.class);

        configure(sysDb, PINECONE, PINECONE_KEY, map("host", storedUrl, "credentials", STORED_SECRET));
        configure(sysDb, QDRANT, QDRANT_KEY, map("host", storedUrl, "credentials", STORED_SECRET));
        configure(sysDb, QDRANT, NO_HOST_KEY, map("credentials", STORED_SECRET));
    }

    @AfterClass
    public static void tearDown() {
        server.close();
        databaseManagementService.shutdown();
    }

    @Before
    public void before() {
        server.clear();
    }

    @Test
    public void storedApiKeyIsNotSentToCallerEndpoint() {
        assertStoredCredentialNotSent(PINECONE_INFO, PINECONE_KEY, map(ENDPOINT_KEY, callerUrl));
    }

    @Test
    public void storedBearerTokenIsNotSentToCallerEndpoint() {
        assertStoredCredentialNotSent(QDRANT_INFO, QDRANT_KEY, map(ENDPOINT_KEY, callerUrl));
    }

    @Test
    public void storedCredentialIsNotSentWithoutStoredHost() {
        assertStoredCredentialNotSent(QDRANT_INFO, NO_HOST_KEY, map(ENDPOINT_KEY, callerUrl));
    }

    @Test
    public void storedApiKeyIsSentToStoredHost() {
        RecordedRequest request = assertInfoSent(PINECONE_INFO, PINECONE_KEY, map());
        assertEquals("/indexes/demo-index", request.path());
        assertEquals(STORED_SECRET, request.header("Api-Key"));
    }

    @Test
    public void storedBearerTokenIsSentToStoredHost() {
        RecordedRequest request = assertInfoSent(QDRANT_INFO, QDRANT_KEY, map());
        assertEquals("/collections/demo-collection", request.path());
        assertEquals("Bearer " + STORED_SECRET, request.header("Authorization"));
    }

    @Test
    public void storedCredentialIsSentToEndpointWithSameOriginAsStoredHost() {
        RecordedRequest request = assertInfoSent(PINECONE_INFO, PINECONE_KEY, map(ENDPOINT_KEY, storedUrl + "/indexes/other"));
        assertEquals("/indexes/other", request.path());
        assertEquals(STORED_SECRET, request.header("Api-Key"));
    }

    @Test
    public void literalHostHonorsCallerEndpoint() {
        RecordedRequest request = assertInfoSent(PINECONE_INFO, storedUrl,
                map(ENDPOINT_KEY, callerUrl + "/custom", HEADERS_KEY, map("Api-Key", "caller-key")));
        assertEquals("/custom", request.path());
        assertEquals("caller-key", request.header("Api-Key"));
    }

    private static void configure(GraphDatabaseService sysDb, VectorDbHandler.Type type, String key, Map<String, Object> conf) {
        sysDb.executeTransactionally("CALL apoc.vectordb.configure($vectorName, $keyConfig, $databaseName, $conf)",
                map("vectorName", type.toString(),
                        "keyConfig", key,
                        "databaseName", DEFAULT_DATABASE_NAME,
                        "conf", conf));
    }

    private static void assertStoredCredentialNotSent(String query, String hostOrKey, Map<String, Object> conf) {
        assertFails(db, query, map("hostOrKey", hostOrKey, "conf", conf), ERROR_UNTRUSTED_ENDPOINT);
        assertEquals(List.of(), server.getRequests());
    }

    private static RecordedRequest assertInfoSent(String query, String hostOrKey, Map<String, Object> conf) {
        testCall(db, query, map("hostOrKey", hostOrKey, "conf", conf),
                r -> assertEquals(Map.of(), r.get("value")));

        List<RecordedRequest> requests = server.getRequests();
        assertEquals(1, requests.size());
        return requests.get(0);
    }
}
