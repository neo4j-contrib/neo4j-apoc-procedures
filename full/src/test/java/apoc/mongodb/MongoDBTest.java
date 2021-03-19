package apoc.mongodb;

import apoc.graph.Graphs;
import apoc.util.JsonUtil;
import apoc.util.MapUtil;
import apoc.util.TestUtil;
import apoc.util.UrlResolver;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.commons.lang3.time.DateUtils;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.*;
import org.junit.rules.ExpectedException;
import org.neo4j.graphdb.*;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.utility.Base58;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

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

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    private static MongoCollection<Document> testCollection;
    private static MongoCollection<Document> productCollection;
    private static MongoCollection<Document> personCollection;

    private static MongoCollection<Document> collection;

    private static final Date currentTime = new Date();

    private static final long longValue = 10_000L;

    private static String HOST = null;

    private static Map<String, Object> params;

    private long numConnections = -1;

    private static final long NUM_OF_RECORDS = 10_000L;

    private static List<ObjectId> productReferences;
    private static ObjectId nameAsObjectId = new ObjectId("507f191e810c19729de860ea");
    private static ObjectId idAsObjectId = new ObjectId();

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
        testCollection = database.getCollection("test");
        productCollection = database.getCollection("product");
        personCollection = database.getCollection("person");
        testCollection.deleteMany(new Document());
        productCollection.deleteMany(new Document());
        LongStream.range(0, NUM_OF_RECORDS)
                .forEach(i -> testCollection.insertOne(new Document(map("name", "testDocument",
                                    "date", currentTime, "longValue", longValue, "nullField", null))));

        productCollection.insertOne(new Document(map("name", "My Awesome Product",
                "price", 800,
                "tags", Arrays.asList("Tech", "Mobile", "Phone", "iOS"))));
        productCollection.insertOne(new Document(map("name", "My Awesome Product 2",
                "price", 1200,
                "tags", Arrays.asList("Tech", "Mobile", "Phone", "Android"))));
        productReferences = StreamSupport.stream(productCollection.find().spliterator(), false)
                .map(doc -> (ObjectId) doc.get("_id"))
                .collect(Collectors.toList());
        personCollection.insertOne(new Document(map("name", "Andrea Santurbano",
                "bought", productReferences,
                "born", DateUtils.parseDate("11-10-1935", "dd-MM-yyyy"),
                "coordinates", Arrays.asList(12.345, 67.890))));
        personCollection.insertOne(new Document(map("name", nameAsObjectId,
                "age", 40,
                "bought", productReferences)));
        personCollection.insertOne(new Document(map("_id", idAsObjectId,
                "name", "Sherlock",
                "age", 25,
                "bought", productReferences)));

        TestUtil.registerProcedure(db, MongoDB.class, Graphs.class);
        mongoClient.close();
    }

    @AfterClass
    public static void tearDown() {
        if (mongo != null) {
            mongo.stop();
        }
    }

    @Rule
    public ExpectedException thrown = ExpectedException.none();

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
    public void shouldExtractObjectIdsAsMaps() {
        boolean hasException = false;
        String url = new UrlResolver("mongodb", mongo.getContainerIpAddress(), mongo.getMappedPort(MONGO_DEFAULT_PORT))
                .getUrl("mongodb", mongo.getContainerIpAddress());
        try (MongoDB.Coll coll = MongoDB.Coll.Factory.create(url, "test", "person", false, true, false)) {
            Map<String, Object> document = coll.first(Collections.emptyMap());
            assertTrue(document.get("_id") instanceof String);
            assertEquals("Andrea Santurbano", document.get("name"));
            assertEquals(Arrays.asList(12.345, 67.890), document.get("coordinates"));
            assertEquals(LocalDateTime.of(1935,10,11, 0, 0), document.get("born"));
            List<Map<String, Object>> bought = (List<Map<String, Object>>) document.get("bought");
            assertEquals(2, bought.size());
            Map<String, Object> product1 = bought.get(0);
            Map<String, Object> product2 = bought.get(1);
            assertTrue(product1.get("_id") instanceof String);
            assertTrue(product2.get("_id") instanceof String);
            assertEquals("My Awesome Product", product1.get("name"));
            assertEquals("My Awesome Product 2", product2.get("name"));
            assertEquals(800, product1.get("price"));
            assertEquals(1200, product2.get("price"));
            assertEquals(Arrays.asList("Tech", "Mobile", "Phone", "iOS"), product1.get("tags"));
            assertEquals(Arrays.asList("Tech", "Mobile", "Phone", "Android"), product2.get("tags"));
        } catch (Exception e) {
            hasException = true;
        }
        assertFalse("should not have an exception", hasException);
    }

    @Test
    public void testObjectIdToStringMapping() {
        boolean hasException = false;
        String url = new UrlResolver("mongodb", mongo.getContainerIpAddress(), mongo.getMappedPort(MONGO_DEFAULT_PORT))
                .getUrl("mongodb", mongo.getContainerIpAddress());
        try (MongoDB.Coll coll = MongoDB.Coll.Factory.create(url, "test", "person", false, false, false)) {
            Map<String, Object> document = coll.first(MapUtil.map("name", "Andrea Santurbano"));
            assertTrue(document.get("_id") instanceof String);
            Collection<String> bought = (Collection<String>) document.get("bought");
            assertEquals(2, bought.size());
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
        try (MongoDB.Coll coll = MongoDB.Coll.Factory.create(url, "test", "test", true, false, true)) {
            Map<String, Object> document = coll.first(MapUtil.map("name", "testDocument"));
            assertNotNull(((Map<String, Object>) document.get("_id")).get("timestamp"));
            assertEquals(LocalDateTime.from(currentTime.toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime()), document.get("date"));
            assertEquals(longValue, document.get("longValue"));
        } catch (Exception e) {
            hasException = true;
        }
        assertFalse("should not have an exception", hasException);
    }

    @Test
    public void testGet()  {
        TestUtil.testResult(db, "CALL apoc.mongodb.get($host,$db,$collection,null)", params,
                res -> assertResult(res));
    }

    @Test
    public void testGetCompatible() throws Exception {
        TestUtil.testResult(db, "CALL apoc.mongodb.get($host,$db,$collection,null,true)", params,
                res -> assertResult(res, LocalDateTime.from(currentTime.toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime())));
    }

    @Test
    public void testFirst() throws Exception {
        TestUtil.testCall(db, "CALL apoc.mongodb.first($host,$db,$collection,{name:'testDocument'})", params, r -> {
            Map doc = (Map) r.get("value");
            assertNotNull(doc.get("_id"));
            assertEquals("testDocument", doc.get("name"));
        });
    }

    @Test
    public void testGetByObjectId() throws Exception {
        List<String> refsIds = productReferences.stream().map(ObjectId::toString).collect(Collectors.toList());
        TestUtil.testCall(db, "CALL apoc.mongodb.get.byObjectId($host,$db,$collection,$id)",
                map("host", HOST, "db", "test", "collection", "person", "id", idAsObjectId.toString()), r -> {
                    Map doc = (Map) r.get("value");
                    assertTrue(doc.get("_id") instanceof Map);
                    assertEquals(
                            Set.of("date", "machineIdentifier", "processIdentifier", "counter", "time", "timestamp", "timeSecond"),
                            ((Map<String, Object>) doc.get("_id")).keySet()
                    );
                    assertEquals("Sherlock", doc.get("name"));
                    assertEquals(25L, doc.get("age"));
                    assertEquals(refsIds, doc.get("bought"));
        });

    }

    @Test
    public void testGetByObjectIdWithCustomIdFieldName() throws Exception {
        List<String> refsIds = productReferences.stream().map(ObjectId::toString).collect(Collectors.toList());

        TestUtil.testCall(db, "CALL apoc.mongodb.get.byObjectId($host, $db, $collection, $objectId, {idFieldName: 'name'})",
                map("host", HOST, "db", "test", "collection", "person", "objectId", nameAsObjectId.toString()), r -> {
                    Map doc = (Map) r.get("value");
                    assertTrue(doc.get("_id") instanceof Map);
                    assertEquals(
                            Set.of("date", "machineIdentifier", "processIdentifier", "counter", "time", "timestamp", "timeSecond"),
                            ((Map<String, Object>) doc.get("_id")).keySet()
                    );
                    assertEquals(40L, doc.get("age"));
                    assertEquals(refsIds, doc.get("bought"));
        });
    }

    @Test
    public void testGetByObjectIdWithConfigs() throws Exception {
        List<String> refsIds = productReferences.stream().map(ObjectId::toString).collect(Collectors.toList());

        TestUtil.testCall(db, "CALL apoc.mongodb.get.byObjectId($host, $db, $collection, $objectId, $config)",
                map("host", HOST, "db", "test", "collection", "person", "objectId", idAsObjectId.toString(),
                        "config", map("extractReferences", true, "objectIdAsMap", false, "compatibleValues", false)), r -> {
                    Map doc = (Map) r.get("value");
                    assertEquals(idAsObjectId.toString(), doc.get("_id"));
                    assertEquals(25, doc.get("age"));
                    assertEquals("Sherlock", doc.get("name"));
                    List<Object> boughtList = (List<Object>) doc.get("bought");
                    Map<String, Object> firstBought = (Map<String, Object>) boughtList.get(0);
                    Map<String, Object> secondBought = (Map<String, Object>) boughtList.get(1);
                    assertEquals(refsIds.get(0), firstBought.get("_id"));
                    assertEquals(refsIds.get(1), secondBought.get("_id"));
                    assertEquals(800, firstBought.get("price"));
                    assertEquals(1200, secondBought.get("price"));
                    assertEquals(Arrays.asList("Tech", "Mobile", "Phone", "iOS"), firstBought.get("tags"));
                    assertEquals(Arrays.asList("Tech", "Mobile", "Phone", "Android"), secondBought.get("tags"));
        });

    }

    @Test
    public void testFind() throws Exception {
        TestUtil.testResult(db, "CALL apoc.mongodb.find($host,$db,$collection,{name:'testDocument'},null,null)",
                params, res -> assertResult(res));
    }

    private void assertResult(Result res) {
        assertResult(res, currentTime.toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime());
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
        TestUtil.testResult(db, "CALL apoc.mongodb.find($host,$db,$collection,{name:'testDocument'},null,{name:1})",
                params, res -> assertResult(res));
    }

    @Test
    public void testCount() throws Exception {
        TestUtil.testCall(db, "CALL apoc.mongodb.count($host,$db,$collection,{name:'testDocument'})", params, r -> {
            assertEquals(NUM_OF_RECORDS, r.get("value"));
        });
    }

    @Test
    public void testCountAll() throws Exception {
        TestUtil.testCall(db, "CALL apoc.mongodb.count($host,$db,$collection,null)", params, r -> {
            assertEquals(NUM_OF_RECORDS, r.get("value"));
        });
    }

    @Test
    public void testUpdate() throws Exception {
        TestUtil.testCall(db, "CALL apoc.mongodb.update($host,$db,$collection,{name:'testDocument'},{`$set`:{age:42}})", params, r -> {
            long affected = (long) r.get("value");
            assertEquals(NUM_OF_RECORDS, affected);
        });
    }

    @Test
    public void testInsert() throws Exception {
        TestUtil.testResult(db, "CALL apoc.mongodb.insert($host,$db,$collection,[{John:'Snow'}])", params, (r) -> {
            assertFalse("should be empty", r.hasNext());
        });
        TestUtil.testCall(db, "CALL apoc.mongodb.first($host,$db,$collection,{John:'Snow'})", params, r -> {
            Map doc = (Map) r.get("value");
            assertNotNull(doc.get("_id"));
            assertEquals("Snow", doc.get("John"));
        });
    }

    @Test
    public void testDelete() throws Exception {
        TestUtil.testResult(db, "CALL apoc.mongodb.insert($host,$db,$collection,[{foo:'bar'}])", params, (r) -> {
            assertFalse("should be empty", r.hasNext());
        });
        TestUtil.testCall(db, "CALL apoc.mongodb.delete($host,$db,$collection,{foo:'bar'})", params, r -> {
            long affected = (long) r.get("value");
            assertEquals(1L, affected);
        });
        TestUtil.testResult(db, "CALL apoc.mongodb.first($host,$db,$collection,{foo:'bar'})", params, r -> {
            assertFalse("should be empty", r.hasNext());
        });
    }

    @Test
    public void testInsertFailsDupKey() {
        // Three apoc.mongodb.insert each call gets the error: E11000 duplicate key error collection
        TestUtil.ignoreException(() -> {
            TestUtil.testResult(db, "CALL apoc.mongodb.insert($host,$db,'error',[{foo:'bar', _id: 1}, {foo:'bar', _id: 1}])", params, (r) -> {
                assertFalse("should be empty", r.hasNext());
            });
        }, QueryExecutionException.class);
        TestUtil.ignoreException(() -> {
            TestUtil.testResult(db, "CALL apoc.mongodb.insert($host,$db,'error',[{foo:'bar', _id: 1}, {foo:'bar', _id: 1}])", params, (r) -> {
                assertFalse("should be empty", r.hasNext());
            });
        }, QueryExecutionException.class);
        TestUtil.ignoreException(() -> {
            TestUtil.testResult(db, "CALL apoc.mongodb.insert($host,$db,'error',[{foo:'bar', _id: 1}, {foo:'bar', _id: 1}])", params, (r) -> {
                assertFalse("should be empty", r.hasNext());
            });
        }, QueryExecutionException.class);
    }

    @Test
    public void shouldInsertDataIntoNeo4jWithFromDocument() throws Exception {
        Date date = DateUtils.parseDate("11-10-1935", "dd-MM-yyyy");
        TestUtil.testResult(db, "CALL apoc.mongodb.first($host, $db, $collection, $filter, $compatibleValues, $extractReferences) YIELD value " +
                        "CALL apoc.graph.fromDocument(value, $fromDocConfig) YIELD graph AS g1 " +
                        "RETURN g1",
                map("host", HOST,
                        "db", "test",
                        "collection", "person",
                        "filter", Collections.emptyMap(),
                        "compatibleValues", true,
                        "extractReferences", true,
                        "fromDocConfig", map("write", true, "skipValidation", true, "mappings", map("$", "Person:Customer{!name,bought,coordinates,born}", "$.bought", "Product{!name,price,tags}"))),
                r -> {
                    Map<String, Object> map = r.next();
                    Map graph = (Map) map.get("g1");
                    assertEquals("Graph", graph.get("name"));
                    Collection<Node> nodes = (Collection<Node>) graph.get("nodes");

                    Map<String, List<Node>> nodeMap = nodes.stream()
                            .collect(Collectors.groupingBy(e -> e.getLabels().iterator().next().name()));
                    List<Node> persons = nodeMap.get("Person");
                    assertEquals(1, persons.size());
                    List<Node> products = nodeMap.get("Product");
                    assertEquals(2, products.size());

                    Node person = persons.get(0);
                    Map<String, Object> personMap = map("name", "Andrea Santurbano",
                            "born", LocalDateTime.ofInstant(date.toInstant(), ZoneId.systemDefault()));
                    assertEquals(personMap, person.getProperties("name", "born"));
                    assertTrue(Arrays.equals(new double[] {12.345, 67.890}, (double[]) person.getProperty("coordinates")));
                    assertEquals(Arrays.asList(Label.label("Person"), Label.label("Customer")), person.getLabels());

                    Node product1 = products.get(0);
                    Map<String, Object> product1Map = map("name", "My Awesome Product",
                            "price", 800L);
                    assertEquals(product1Map, product1.getProperties("name", "price"));
                    assertArrayEquals(new String[]{"Tech", "Mobile", "Phone", "iOS"}, (String[]) product1.getProperty("tags"));
                    assertEquals(Arrays.asList(Label.label("Product")), product1.getLabels());

                    Node product2 = products.get(1);
                    Map<String, Object> product2Map = map("name", "My Awesome Product 2",
                            "price", 1200L);
                    assertEquals(product2Map, product2.getProperties("name", "price"));
                    assertArrayEquals(new String[]{"Tech", "Mobile", "Phone", "Android"}, (String[]) product2.getProperty("tags"));
                    assertEquals(Arrays.asList(Label.label("Product")), product2.getLabels());


                    Collection<Relationship> rels = (Collection<Relationship>) graph.get("relationships");
                    assertEquals(2, rels.size());
                    Iterator<Relationship> relIt = rels.iterator();
                    Relationship rel1 = relIt.next();
                    assertEquals(RelationshipType.withName("BOUGHT"), rel1.getType());
                    assertEquals(person, rel1.getStartNode());
                    assertEquals(product1, rel1.getEndNode());
                    Relationship rel2 = relIt.next();
                    assertEquals(RelationshipType.withName("BOUGHT"), rel2.getType());
                    assertEquals(person, rel2.getStartNode());
                    assertEquals(product2, rel2.getEndNode());

                });
    }

    @Ignore("this does not throw an exception")  //TODO: check why test is failing
    @Test
    public void shouldFailTheInsertWithoutCompatibleValuesFlag() {
        thrown.expect(QueryExecutionException.class);
        TestUtil.testResult(db, "CALL apoc.mongodb.first($host, $db, $collection, $filter, $compatibleValues, $extractReferences) YIELD value " +
                        "CALL apoc.graph.fromDocument(value, $fromDocConfig) YIELD graph AS g1 " +
                        "RETURN g1",
                map("host", HOST,
                        "db", "test",
                        "collection", "person",
                        "filter", Collections.emptyMap(),
                        "compatibleValues", false,
                        "extractReferences", true,
                        "fromDocConfig", map("write", true, "skipValidation", true, "mappings", map("$", "Person:Customer{!name,bought,coordinates,born}", "$.bought", "Product{!name,price,tags}"))),
                r -> {});
    }

    @Test
    public void shouldUseMongoUrlKey() {
        TestUtil.testResult(db, "CALL apoc.mongodb.first($host, $db, $collection, $filter, $compatibleValues, $extractReferences)",
                map("host", HOST,
                        "db", "test",
                        "collection", "person",
                        "filter", Collections.emptyMap(),
                        "compatibleValues", false,
                        "extractReferences", true),
                r -> assertTrue(r.hasNext()));
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
