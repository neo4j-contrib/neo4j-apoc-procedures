package apoc.mongodb;

import apoc.graph.Graphs;
import apoc.util.TestUtil;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.commons.lang3.time.DateUtils;
import org.bson.BsonInt64;
import org.bson.BsonRegularExpression;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.types.Binary;
import org.bson.types.Code;
import org.bson.types.CodeWithScope;
import org.bson.types.MaxKey;
import org.bson.types.MinKey;
import org.bson.types.ObjectId;
import org.bson.types.Symbol;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Result;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.StreamSupport;

import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testResult;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class MongoTest extends MongoTestBase {

    private static int MONGO_DEFAULT_PORT = 27017;
    private static final String[] COMMANDS = { "mongo", "admin", "--eval", "db.auth('user', 'pass'); db.serverStatus().connections;" };

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    private static MongoCollection<Document> testCollection;
    private static MongoCollection<Document> productCollection;
    private static MongoCollection<Document> personCollection;
    private static MongoCollection<Document> barCollection;

    private static final Date currentTime = new Date();

    private static final long longValue = 10_000L;

    private static String URI = null;

    public static Map<String, Object> PERSON_PARAMS;
    private static Map<String, Object> TEST_PARAMS;

    private static Map<String, Object> URI_PARAM;
    private static Map<String, Object> URI_WITH_AUTH_SOURCE_PARAM;

    private long numConnections = -1;

    private static final long NUM_OF_RECORDS = 10_000L;

    private static List<ObjectId> productReferences;
    private static ObjectId nameAsObjectId = new ObjectId("507f191e810c19729de860ea");
    private static ObjectId fooAlAsObjectId = new ObjectId("77e193d7a9cc81b4027498b4");
    private static ObjectId fooJohnAsObjectId = new ObjectId("57e193d7a9cc81b4027499c4");
    private static ObjectId fooJackAsObjectId = new ObjectId("67e193d7a9cc81b4027518b4");
    private static ObjectId idAlAsObjectId = new ObjectId("97e193d7a9cc81b4027519b4");
    private static ObjectId idJackAsObjectId = new ObjectId("97e193d7a9cc81b4027518b4");
    private static ObjectId idJohnAsObjectId = new ObjectId("07e193d7a9cc81b4027488c4");
    private static ObjectId idAsObjectId = new ObjectId();

    private static final Set<String> SET_OBJECT_ID_MAP = Set.of("date", "machineIdentifier", "processIdentifier", "counter", "time", "timestamp", "timeSecond");

    @BeforeClass
    public static void setUp() throws Exception {
        createContainer(true);

        final String host = mongo.getHost();
        final Integer port = mongo.getMappedPort(MONGO_DEFAULT_PORT);
        final String format = String.format("mongodb://user:pass@%s:%s", host, port);
        // TODO - A QUESTO APPENDERE NOMEDB.COLLECTION NELLE PROCEDURE e controllare che se non ci sono devo fare boom
        MongoClientURI uri = new MongoClientURI(format);
        MongoClient mongoClient = new MongoClient(uri);
//        MongoClient mongoClient = new MongoClient(mongo.getContainerIpAddress(), mongo.getMappedPort(MONGO_DEFAULT_PORT));

        URI = String.format("mongodb://user:pass@%s:%s", mongo.getContainerIpAddress(), mongo.getMappedPort(MONGO_DEFAULT_PORT));
//        HOST = String.format("mongodb://%s:%s", mongo.getContainerIpAddress(), mongo.getMappedPort(MONGO_DEFAULT_PORT));


        URI_PARAM = map("uri", URI);
        // todo - fare funzione che appenda e restituisca la mappa
//        String suffix = "/test.test?authSource";
//        getUriParam(suffix);


        TEST_PARAMS = map("host", URI, "db", "test", "collection", "test");
        PERSON_PARAMS = map("host", URI, "db", "test", "collection", "person");
        MongoDatabase database = mongoClient.getDatabase("test");
        testCollection = database.getCollection("test");
//        database.createCollection();
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
        personCollection.insertOne(new Document(map("_id", idAlAsObjectId,
                "name", "Al",
                "age", 25,
                "foo", fooAlAsObjectId,
                "date", new Date(1234567),
                "int64", new BsonInt64(29L),
                "double", 12.34,
                "expr", new BsonRegularExpression("foo*"),
                "binary", new Binary("fooBar".getBytes()),
                "bought", productReferences)));
        personCollection.insertOne(new Document(map("_id", idJohnAsObjectId,
                "name", "John",
                "age", 45,
                "foo", fooJohnAsObjectId,
                "code", new Code("function() {}"),
                "sym", new Symbol("x"),
                "codeWithScope", new CodeWithScope("function() {}", new Document("k", "v")),
                "int64", new BsonInt64(29L),
                "time", new BsonTimestamp(123, 4),
                "minKey", new MinKey(),
                "maxKey", new MaxKey(),
                "expr", new BsonRegularExpression("foo*"),
                "binary", new Binary("fooBar".getBytes()),
                "bought", productReferences)));
        personCollection.insertOne(new Document(map("_id", idJackAsObjectId,
                "name", "Jack",
                "age", 54,
                "foo", fooJackAsObjectId,
                "expr", new BsonRegularExpression("foo*"),
                "minKey", new MinKey(),
                "maxKey", new MaxKey(),
                "bought", productReferences)));


        MongoDatabase databaseFoo = mongoClient.getDatabase("foo");
        barCollection = databaseFoo.getCollection("bar");
        barCollection.insertOne(new Document(map("name", nameAsObjectId,
                "age", 54)));

        TestUtil.registerProcedure(db, Mongo.class, Graphs.class);
        mongoClient.close();
    }

    private static Map<String, Object> getParams(String suffix) {
        return map("uri", URI + suffix);
    }

//    private static Map<String, Object> getParams(String suffix, Map<String, Object> config) {
//        final Map<String, Object> params = getParams(suffix);
//        params.put("config", config);
//        return params;
//    }

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
        numConnections = (long) getNumConnections(mongo, COMMANDS).get("current");
    }

    @After
    public void after() {
        // the connections active before must be equal to the connections active after
        long numConnectionsAfter = (long) getNumConnections(mongo, COMMANDS).get("current");
        assertEquals(numConnections, numConnectionsAfter);
    }


    // todo - test connection without username e password --> che succede
    @Test//(expected = QueryExecutionException.class)
    public void shouldFailIfUriHasNotCredentials() {
//        testResult(db, "CALL apoc.mongo.get($uri, {objectIdAsMap: false})",
//                map("uri", String.format("mongodb://%s:%s/test.test?authSource=admin", mongo.getContainerIpAddress(), mongo.getMappedPort(MONGO_DEFAULT_PORT))),
//                res -> {
//            // todo
//        });

        final String bytes = Base64.getEncoder().encodeToString("fooBar".getBytes());
        try {
            testCall(db, "CALL apoc.mongo.get($uri, {query: {binary: {`$binary`: $bytes, `$subType`: '00'}}})",
                    map("uri", String.format("mongodb://%s:%s/test.test?authSource=admin", mongo.getContainerIpAddress(), mongo.getMappedPort(MONGO_DEFAULT_PORT)), "bytes", bytes),
                    row -> {});
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("not authorized on test to execute command"));
//            throw e;
        }

//        testResult(db, "CALL apoc.mongo.get($uri, {query: {binary: {`$binary`: $bytes, `$subType`: '00'}}})",
////        testResult(db, "CALL apoc.mongo.get($uri,{compatibleValues: true})",
//                map("uri", String.format("mongodb://%s:%s/test.test?authSource=admin", mongo.getContainerIpAddress(), mongo.getMappedPort(MONGO_DEFAULT_PORT)), "bytes", bytes),
//                res -> {
//                    int count = 0;
//                    while (res.hasNext()) {
//                        ++count;
//                        Map<String, Object> r = res.next();
//                        Map doc = (Map) r.get("value");
//                        assertTrue(doc.get("_id") instanceof Map);
//                        assertEquals(SET_OBJECT_ID_MAP, ((Map<String, Object>) doc.get("_id")).keySet());
//                        assertTrue(List.of("Al", "John").contains(doc.get("name")));
//                        assertTrue(List.of(25L, 45L).contains(doc.get("age")));
//                        final List<String> expectedObjIds = List.of("77e193d7a9cc81b4027498b4", "57e193d7a9cc81b4027499c4", "67e193d7a9cc81b4027518b4");
//                        assertTrue(expectedObjIds.contains(doc.get("foo")));
//                    }
//                    assertEquals(2, count);
//                });


        // todo - lista di valori non compatibili di person
//        boolean hasException = false;
//        String url = new UrlResolver("mongodb", mongo.getContainerIpAddress(), mongo.getMappedPort(MONGO_DEFAULT_PORT))
//                .getUrl("mongodb", mongo.getContainerIpAddress());
//        try (MongoDB.Coll coll = MongoDB.Coll.Factory.create(url, "test", "test", true, false, true)) {
//            Map<String, Object> document = coll.first(map("name", "testDocument"));
//            assertNotNull(((Map<String, Object>) document.get("_id")).get("timestamp"));
//            assertEquals(LocalDateTime.from(currentTime.toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime()), document.get("date"));
//            assertEquals(longValue, document.get("longValue"));
//        } catch (Exception e) {
//            hasException = true;
//        }
//        assertFalse("should not have an exception", hasException);
    }

    @Test//(expected = QueryExecutionException.class)
    public void shouldFailIfUriHasNotDatabase() {
        try {
            testCall(db, "CALL apoc.mongo.first($uri,{name:'testDocument'})",
                    map("uri", String.format("mongodb://user:pass@%s:%s?authSource=admin", mongo.getContainerIpAddress(), mongo.getMappedPort(MONGO_DEFAULT_PORT))),
                    r -> {});
        } catch (Exception e) {
            System.out.println("MongoTest.shouldFailIfUriHasNotDatabase");
        }
    }

    // todo - test connection with admin database con user e passord

    // todo - test connection without database

    // todo - test connection without collection

    // todo - test connection without collection

    // todo - test connection with timeout e Thread.sleep

    @Test
    public void objectIdAsString() {
        testResult(db, "CALL apoc.mongo.get($uri, {objectIdAsMap: false})", getParams("/test.person?authSource=admin"), res -> {
            // todo
        });
    }

    @Test
    public void testCompatibleValues() {
        // todo - lista di valori non compatibili di person
//        boolean hasException = false;
//        String url = new UrlResolver("mongodb", mongo.getContainerIpAddress(), mongo.getMappedPort(MONGO_DEFAULT_PORT))
//                .getUrl("mongodb", mongo.getContainerIpAddress());
//        try (MongoDB.Coll coll = MongoDB.Coll.Factory.create(url, "test", "test", true, false, true)) {
//            Map<String, Object> document = coll.first(map("name", "testDocument"));
//            assertNotNull(((Map<String, Object>) document.get("_id")).get("timestamp"));
//            assertEquals(LocalDateTime.from(currentTime.toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime()), document.get("date"));
//            assertEquals(longValue, document.get("longValue"));
//        } catch (Exception e) {
//            hasException = true;
//        }
//        assertFalse("should not have an exception", hasException);
    }


    @Test
    public void testWithSkip() {

    }

    @Test
    public void testWithLimit() {

    }

    @Test
    public void testWithSkipAndLimit() {

    }

    @Test
    public void testFindWithSkipAndLimit() {

    }

    @Test
    public void testFindWithProject() {

    }

    @Test
    public void testGet()  {
        testResult(db, "CALL apoc.mongo.get($uri)", getParams("/test.test?authSource=admin"), this::assertResult);
    }

    @Test
    // todo - fail
    public void testGetWithComplexTypes()  {
        final String bytes = Base64.getEncoder().encodeToString("fooBar".getBytes());
        testResult(db, "CALL apoc.mongo.get($uri, {query: {binary: {`$binary`: $bytes, `$subType`: '00'}}})",
//        testResult(db, "CALL apoc.mongo.get($uri,{compatibleValues: true})",
                map("uri", URI + "/test.person?authSource=admin", "bytes", bytes),
                res -> {
                    int count = 0;
                    while (res.hasNext()) {
                        ++count;
                        Map<String, Object> r = res.next();
                        Map doc = (Map) r.get("value");
                        assertTrue(doc.get("_id") instanceof Map);
                        assertEquals(SET_OBJECT_ID_MAP, ((Map<String, Object>) doc.get("_id")).keySet());
                        assertTrue(List.of("Al", "John").contains(doc.get("name")));
                        assertTrue(List.of(25L, 45L).contains(doc.get("age")));
                        final List<String> expectedObjIds = List.of("77e193d7a9cc81b4027498b4", "57e193d7a9cc81b4027499c4", "67e193d7a9cc81b4027518b4");
                        assertTrue(expectedObjIds.contains(doc.get("foo")));
                    }
                    assertEquals(2, count);
                });

        // todo - get / find / first con altri tipi dato
    }

    @Test
    public void testFirst() {
        testCall(db, "CALL apoc.mongo.first($uri,{name:'testDocument'})", getParams("/test.test?authSource=admin"), r -> {
            Map<String, Object> doc = (Map<String, Object>) r.get("value");
            assertNotNull(doc.get("_id"));
            assertEquals("testDocument", doc.get("name"));
        });
    }

    @Test(expected = QueryExecutionException.class)
    public void testFailsWithExtendedJsonFalseWithObjectIdParam() {
        try {
            testCall(db, "CALL apoc.mongo.first($uri,{query: {`_id`: {`$oid`: '97e193d7a9cc81b4027519b4'}}, useExtendedJson: false})",
                    getParams("/test.person?authSource=admin"), r -> {});
        } catch (QueryExecutionException e) {
            assertTrue(e.getMessage().contains("unknown operator: $oid"));
            throw e;
        }
    }

    @Test
    // todo - test con questo vero e compatibleValue falso e vedere che fa boom
    public void testFirstWithExtendedJson() {
        List<String> refsIds = productReferences.stream().map(ObjectId::toString).collect(Collectors.toList());
        testCall(db, "CALL apoc.mongo.first($uri,{query: {`_id`: {`$oid`: '97e193d7a9cc81b4027519b4'}}})",
                getParams("/test.person?authSource=admin"), r -> {
                    Map doc = (Map) r.get("value");
                    assertTrue(doc.get("_id") instanceof Map);
                    assertEquals(SET_OBJECT_ID_MAP, ((Map<String, Object>) doc.get("_id")).keySet());
                    assertEquals(25L, doc.get("age"));
                    assertEquals("foo*", doc.get("expr"));
                    assertEquals("fooBar", new String((byte[]) doc.get("binary")));
                    assertEquals(12.34, doc.get("double"));
                    assertEquals( 29L, doc.get("int64"));
                    assertEquals("77e193d7a9cc81b4027498b4", doc.get("foo") );
                    assertEquals(refsIds, doc.get("bought"));
        });
    }

    @Test
    public void testFind() throws Exception {
//        testResult(db, "CALL apoc.mongodb.find($host,$db,$collection,{name:'testDocument'},null,null)",
        testResult(db, "CALL apoc.mongo.find($uri)", getParams("/test.test?authSource=admin"), this::assertResult);
    }

    @Test
    public void testFindWithExtendedJson() throws Exception {
        final String bytes = Base64.getEncoder().encodeToString("fooBar".getBytes());
        testResult(db, "CALL apoc.mongo.find($uri,{query: {binary: {`$binary`: $bytes, `$subType`: '00'}}})",
                map("uri", URI + "/test.person?authSource=admin", "bytes", bytes),
//                getParams("/person.test?authSource=admin", "bytes", bytes),
//                map("host", URI, "db", "test", "collection", "person", "bytes", bytes),
                res -> {
                    int count = 0;
                    while (res.hasNext()) {
                        ++count;
                        Map<String, Object> r = res.next();
                        Map doc = (Map) r.get("value");
                        assertTrue(List.of("Al", "John").contains(doc.get("name")));
                        assertEquals("foo*", doc.get("expr"));
                        assertTrue(List.of(25L, 45L).contains(doc.get("age")));
                        final List<String> expectedObjIds = List.of("97e193d7a9cc81b4027519b4", "07e193d7a9cc81b4027488c4");
                        final Map<String, Object> id = (Map<String, Object>) doc.get("_id");
                        assertEquals(SET_OBJECT_ID_MAP, id.keySet());
                        final List<String> expectedObjIdsFooProp = List.of("77e193d7a9cc81b4027498b4", "57e193d7a9cc81b4027499c4");
                        assertTrue(expectedObjIdsFooProp.contains(doc.get("foo")));
                    }
                    assertEquals(2, count);
                });
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
        testResult(db, "CALL apoc.mongo.find($uri,{query: {name:'testDocument'}, sort: {name:1}})", getParams("/test.test?authSource=admin"), this::assertResult);
    }

    @Test
    public void testCountWithExtendedJson() throws Exception {
        final String bytes = Base64.getEncoder().encodeToString("fooBar".getBytes());
        testCall(db, "CALL apoc.mongo.count($uri,{query: {binary: {`$binary`: 'Zm9vQmFy', `$subType`: '00'}, int64: {`$numberLong`: '29'}}})",
                map("uri", URI + "/test.person?authSource=admin", "bytes", bytes),
//                map("host", URI, "db", "test", "collection", "person", "bytes", bytes),
                r -> assertEquals(2L, r.get("value")));
    }

    @Test
    public void testCountAll() throws Exception {
        // ?? perché 10001
        testCall(db, "CALL apoc.mongo.count($uri,{query: {name: 'testDocument'}})", getParams("/test.test?authSource=admin"),
                r -> assertEquals(NUM_OF_RECORDS, r.get("value")));
    }

    @Test
    public void testUpdateWithExtendedJson() throws Exception {
        testCall(db, "CALL apoc.mongo.update($uri,{foo: {`$oid`: '57e193d7a9cc81b4027499c4'}},{`$set`:{code: {`$code`: 'void 0'}}})",
                getParams("/test.person?authSource=admin"), r -> {
            long affected = (long) r.get("value");
            assertEquals(1, affected);
        });
    }

    @Test
    public void testInsertWithExtendedJson() throws Exception {
        testResult(db, "CALL apoc.mongo.insert($uri,[{secondId: {`$oid` : '507f191e811c19729de860ea'}, baz: 1}, {secondId: {`$oid` : '507f191e821c19729de860ef'}, baz: 1}])",
                map("uri", URI + "/test.person?authSource=admin"), (r) -> {
            assertFalse("should be empty", r.hasNext());
        });
        testCall(db, "CALL apoc.mongo.count($uri,{query: {baz:1}})", getParams("/test.person?authSource=admin"), r -> {
            long affected = (long) r.get("value");
            assertEquals(2L, affected);
        });
    }

    @Test
    public void testInsertRegexExtJsonGetFirstCorrectlyWithCompatibleValueTrueAndFailsIfFalse() {
        testResult(db, "CALL apoc.mongo.insert($uri,[{foo:{`$regex`: 'pattern', `$options`: ''}, myId: {`$oid` : '507f191e811c19729de960ea'}}])",
                getParams("/test.person?authSource=admin"), (r) -> assertFalse("should be empty", r.hasNext()));
        try {
            testCall(db, "CALL apoc.mongo.first($uri, {query: {foo:{ `$regex`: 'pattern', `$options`: '' }}, compatibleValues: false})",
                    getParams("/test.person?authSource=admin"), r -> {});
            fail();
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("java.lang.IllegalArgumentException: Cannot convert BsonRegularExpression"));
        }

        testCall(db, "CALL apoc.mongo.first($uri, {query: {foo:{ `$regex`: 'pattern', `$options`: ''}}})", getParams("/test.person?authSource=admin"), r -> {
            Map<String, Object> value = (Map<String, Object>) r.get("value");
            assertEquals("pattern", value.get("foo"));
            assertTrue(value.get("_id") instanceof Map);
            assertEquals(SET_OBJECT_ID_MAP, ((Map<String, Object>) value.get("_id")).keySet());
            assertEquals("507f191e811c19729de960ea", value.get("myId"));
        });

        // todo
        testCall(db, "CALL apoc.mongo.delete($uri, {foo: {`$regex`: 'pattern', `$options`: ''}})", getParams("/test.person?authSource=admin"), r -> {
            long affected = (long) r.get("value");
            assertEquals(1L, affected);
        });
    }

    // todo
    @Test
    public void testGetInsertAndDeleteWithExtendedJson() throws Exception {
        final String suffix = "/test.person?authSource=admin";
        testResult(db, "CALL apoc.mongo.insert($uri,[{foo:'bar', myId: {`$oid` : '507f191e811c19729de960ea'}}])", getParams(suffix), (r) -> {
            assertFalse("should be empty", r.hasNext());
        });
        testCall(db, "CALL apoc.mongo.delete($uri,{foo: 'bar', myId: {`$oid` : '507f191e811c19729de960ea'}})", getParams(suffix), r -> {
            long affected = (long) r.get("value");
            assertEquals(1L, affected);
        });
        testResult(db, "CALL apoc.mongo.first($uri,{query: {foo:'bar', myId: {`$oid` : '507f191e811c19729de960ea'}}})", getParams(suffix), r -> {
            assertFalse("should be empty", r.hasNext());
        });
    }

    @Test
    public void testInsertFailsDupKey() {
        try {
            testResult(db, "CALL apoc.mongo.insert($uri,[{foo:'bar', _id: 1}, {foo:'bar', _id: 1}])", getParams("/test.test?authSource=admin"), (r) -> {
                assertFalse("should be empty", r.hasNext());
            });
            fail("Should fail because of duplicate key");
        } catch (QueryExecutionException e) {
            assertTrue(e.getMessage().contains("E11000 duplicate key error collection"));
        }
    }

    // todo - lasciarlo e fare assertion comune
    @Test
    public void shouldInsertDataIntoNeo4jWithFromDocument() throws Exception {
        Date date = DateUtils.parseDate("11-10-1935", "dd-MM-yyyy");
        testResult(db, "CALL apoc.mongo.first($uri, {query: {}, extractReferences: true}) YIELD value " +
                        "CALL apoc.graph.fromDocument(value, $fromDocConfig) YIELD graph AS g1 " +
                        "RETURN g1",
                map("uri", URI + "/test.person?authSource=admin",
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
}
