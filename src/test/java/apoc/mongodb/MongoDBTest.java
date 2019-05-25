package apoc.mongodb;

import apoc.util.JsonUtil;
import apoc.util.MapUtil;
import apoc.util.TestUtil;
import apoc.util.UrlResolver;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.junit.*;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Result;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.utility.Base58;

import java.time.Duration;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.isTravis;
import static java.net.HttpURLConnection.HTTP_OK;
import static java.net.HttpURLConnection.HTTP_UNAUTHORIZED;
import static org.junit.Assert.*;
import static org.junit.Assume.*;

/**
 * @author mh
 * @since 30.06.16
 */
public class MongoDBTest {

    private static int MONGO_DEFAULT_PORT = 27017;

    public static GenericContainer mongo;

    private static GraphDatabaseService db;
    private static MongoCollection<Document> collection;

    private static final Date currentTime = new Date();

    private static final long longValue = 10_000L;

    private static String HOST = null;

    private static Map<String, Object> params;

    private long numConnections = -1;

    private static final long NUM_OF_RECORDS = 10_000L;

    @BeforeClass
    public static void setUp() throws Exception {
        assumeFalse(isTravis());
        TestUtil.ignoreException(() -> {
            mongo = new GenericContainer("mongo:3")
                    .withNetworkAliases("mongo-" + Base58.randomString(6))
                    .withExposedPorts(MONGO_DEFAULT_PORT)
                    .waitingFor(new HttpWaitStrategy()
                            .forPort(MONGO_DEFAULT_PORT)
                            .forStatusCodeMatching(response -> response == HTTP_OK || response == HTTP_UNAUTHORIZED)
                            .withStartupTimeout(Duration.ofMinutes(2)));
            mongo.start();

        }, Exception.class);
        assumeNotNull(mongo);
        assumeTrue("Mongo DB must be running", mongo.isRunning());
        MongoClient mongoClient = new MongoClient(mongo.getContainerIpAddress(), mongo.getMappedPort(MONGO_DEFAULT_PORT));
        HOST = String.format("mongodb://%s:%s", mongo.getContainerIpAddress(), mongo.getMappedPort(MONGO_DEFAULT_PORT));
        params = map("host", HOST, "db", "test", "collection", "test");
        MongoDatabase database = mongoClient.getDatabase("test");
        collection = database.getCollection("test");
        collection.deleteMany(new Document());
        LongStream.range(0, NUM_OF_RECORDS)
                .forEach(i -> collection
                        .insertOne(new Document(map("name", "testDocument",
                                "date", currentTime, "longValue", longValue))));
//        collection.insertOne(new Document(map("name", "testDocument", "date", currentTime, "longValue", longValue)));
        db = new TestGraphDatabaseFactory()
                .newImpermanentDatabaseBuilder()
                .newGraphDatabase();
        TestUtil.registerProcedure(db, MongoDB.class);
        mongoClient.close();
    }

    @AfterClass
    public static void tearDown() {
        if (mongo != null) {
            mongo.stop();
            db.shutdown();
        }
    }

    @Before
    public void before() {
        numConnections = (long) getNumConnections().get("current");
    }

    @After
    public void after() {
        // the connections active before must be equal to the connections active after
        long numConnectionsAfter = (long) getNumConnections().get("current");
        assertEquals(numConnections, numConnectionsAfter);
    }

    @Test
    public void testObjectIdToStringMapping() {
        boolean hasException = false;
        String url = new UrlResolver("mongodb", mongo.getContainerIpAddress(), mongo.getMappedPort(MONGO_DEFAULT_PORT))
                .getUrl("mongodb", mongo.getContainerIpAddress());
        try (MongoDB.Coll coll = MongoDB.Coll.Factory.create(url, "test", "test", false)) {
            Map<String, Object> document = coll.first(MapUtil.map("name", "testDocument"));
            assertTrue(document.get("_id") instanceof String);
        } catch (Exception e) {
            hasException = true;
        }
        assertFalse("should not have an exception", hasException);
    }

    @Test
    public void testCompatibleValues() {
        boolean hasException = false;
        String url = new UrlResolver("mongodb", mongo.getContainerIpAddress(), mongo.getMappedPort(MONGO_DEFAULT_PORT))
                .getUrl("mongodb", mongo.getContainerIpAddress());
        try (MongoDB.Coll coll = MongoDB.Coll.Factory.create(url, "test", "test", true)) {
            Map<String, Object> document = coll.first(MapUtil.map("name", "testDocument"));
            assertNotNull(((Map<String, Object>) document.get("_id")).get("timestamp"));
            assertEquals(currentTime.getTime(), document.get("date"));
            assertEquals(longValue, document.get("longValue"));
        } catch (Exception e) {
            hasException = true;
        }
        assertFalse("should not have an exception", hasException);
    }

    @Test
    public void testGet()  {
        TestUtil.testResult(db, "CALL apoc.mongodb.get({host},{db},{collection},null)", params,
                res -> assertResult(res));
    }

    @Test
    public void testGetCompatible() throws Exception {
        TestUtil.testResult(db, "CALL apoc.mongodb.get({host},{db},{collection},null,true)", params,
                res -> assertResult(res, currentTime.getTime()));
    }

    @Test
    public void testFirst() throws Exception {
        TestUtil.testCall(db, "CALL apoc.mongodb.first({host},{db},{collection},{name:'testDocument'})", params, r -> {
            Map doc = (Map) r.get("value");
            assertNotNull(doc.get("_id"));
            assertEquals("testDocument", doc.get("name"));
        });
    }

    @Test
    public void testFind() throws Exception {
        TestUtil.testResult(db, "CALL apoc.mongodb.find({host},{db},{collection},{name:'testDocument'},null,null)",
                params, res -> assertResult(res));
    }

    private void assertResult(Result res) {
        assertResult(res, currentTime);
    }

    private void assertResult(Result res, Object date) {
        int count = 0;
        while (res.hasNext()) {
            ++count;
            Map<String, Object> r = res.next();
            Map doc = (Map) r.get("value");
            assertNotNull(doc.get("_id"));
            assertEquals("testDocument", doc.get("name"));
            assertEquals(date, doc.get("date"));
            assertEquals(longValue, doc.get("longValue"));
        }
        assertEquals(NUM_OF_RECORDS, count);
    }

    @Test
    public void testFindSort() throws Exception {
        TestUtil.testResult(db, "CALL apoc.mongodb.find({host},{db},{collection},{name:'testDocument'},null,{name:1})",
                params, res -> assertResult(res));
    }

    @Test
    public void testCount() throws Exception {
        TestUtil.testCall(db, "CALL apoc.mongodb.count({host},{db},{collection},{name:'testDocument'})", params, r -> {
            assertEquals(NUM_OF_RECORDS, r.get("value"));
        });
    }

    @Test
    public void testCountAll() throws Exception {
        TestUtil.testCall(db, "CALL apoc.mongodb.count({host},{db},{collection},null)", params, r -> {
            assertEquals(NUM_OF_RECORDS, r.get("value"));
        });
    }

    @Test
    public void testUpdate() throws Exception {
        TestUtil.testCall(db, "CALL apoc.mongodb.update({host},{db},{collection},{name:'testDocument'},{`$set`:{age:42}})", params, r -> {
            long affected = (long) r.get("value");
            assertEquals(NUM_OF_RECORDS, affected);
        });
    }

    @Test
    public void testInsert() throws Exception {
        TestUtil.testResult(db, "CALL apoc.mongodb.insert({host},{db},{collection},[{John:'Snow'}])", params, (r) -> {
            assertFalse("should be empty", r.hasNext());
        });
        TestUtil.testCall(db, "CALL apoc.mongodb.first({host},{db},{collection},{John:'Snow'})", params, r -> {
            Map doc = (Map) r.get("value");
            assertNotNull(doc.get("_id"));
            assertEquals("Snow", doc.get("John"));
        });
    }

    @Test
    public void testDelete() throws Exception {
        TestUtil.testResult(db, "CALL apoc.mongodb.insert({host},{db},{collection},[{foo:'bar'}])", params, (r) -> {
            assertFalse("should be empty", r.hasNext());
        });
        TestUtil.testCall(db, "CALL apoc.mongodb.delete({host},{db},{collection},{foo:'bar'})", params, r -> {
            long affected = (long) r.get("value");
            assertEquals(1L, affected);
        });
        TestUtil.testResult(db, "CALL apoc.mongodb.first({host},{db},{collection},{foo:'bar'})", params, r -> {
            assertFalse("should be empty", r.hasNext());
        });
    }

    @Test
    public void testInsertFailsDupKey() {
        // Three apoc.mongodb.insert each call gets the error: E11000 duplicate key error collection
        TestUtil.ignoreException(() -> {
            TestUtil.testResult(db, "CALL apoc.mongodb.insert({host},{db},'error',[{foo:'bar', _id: 1}, {foo:'bar', _id: 1}])", params, (r) -> {
                assertFalse("should be empty", r.hasNext());
            });
        }, QueryExecutionException.class);
        TestUtil.ignoreException(() -> {
            TestUtil.testResult(db, "CALL apoc.mongodb.insert({host},{db},'error',[{foo:'bar', _id: 1}, {foo:'bar', _id: 1}])", params, (r) -> {
                assertFalse("should be empty", r.hasNext());
            });
        }, QueryExecutionException.class);
        TestUtil.ignoreException(() -> {
            TestUtil.testResult(db, "CALL apoc.mongodb.insert({host},{db},'error',[{foo:'bar', _id: 1}, {foo:'bar', _id: 1}])", params, (r) -> {
                assertFalse("should be empty", r.hasNext());
            });
        }, QueryExecutionException.class);
    }

    /**
     * Get the number of active connections that can be used to check if them are managed correctly
     * by invoking:
     *  $ mongo test --eval db.serverStatus().connections
     * into the container
     * @return A Map<String, Long> with three fields three fields {current, available, totalCreated}
     * @throws Exception
     */
    public static Map<String, Object> getNumConnections() {
        try {
            Container.ExecResult execResult = mongo.execInContainer("mongo", "test", "--eval", "db.serverStatus().connections");
            assertTrue("stderr is empty", execResult.getStderr() == null || execResult.getStderr().isEmpty());
            assertTrue("stdout is not empty", execResult.getStdout() != null && !execResult.getStdout().isEmpty());

            List<String> lists = Stream.of(execResult.getStdout().split("\n"))
                    .filter(s -> s != null || !s.isEmpty())
                    .collect(Collectors.toList());
            String jsonStr = lists.get(lists.size() - 1);
            return JsonUtil.OBJECT_MAPPER.readValue(jsonStr, Map.class);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
