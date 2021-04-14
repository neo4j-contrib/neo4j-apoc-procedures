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
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Base64;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.StreamSupport;

import static apoc.mongodb.MongoDBColl.ERROR_MESSAGE;
import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testResult;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class MongoTest extends MongoTestBase {

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    private static MongoCollection<Document> testCollection;
    private static MongoCollection<Document> productCollection;
    private static MongoCollection<Document> personCollection;
    private static MongoCollection<Document> barCollection;

    private static final Date currentTime = new Date();

    private static final long longValue = 10_000L;

    private static String URI = null;

    private static List<String> BOUGHT_REFS;


    private static final long NUM_OF_RECORDS = 10_000L;

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

        // readWrite user creation
        mongo.execInContainer("mongo", "admin", "--eval", "db.auth('admin', 'pass'); db.createUser({user: 'user', pwd: 'secret',roles: [{role: 'readWrite', db: 'test'}]});");

        final String host = mongo.getHost();
        final Integer port = mongo.getMappedPort(MONGO_DEFAULT_PORT);
        final String format = String.format("mongodb://admin:pass@%s:%s", host, port);
        MongoClientURI uri = new MongoClientURI(format);
        MongoClient mongoClient = new MongoClient(uri);

        URI = String.format("mongodb://admin:pass@%s:%s", mongo.getContainerIpAddress(), mongo.getMappedPort(MONGO_DEFAULT_PORT));

        // TODO - ESTRAPOLARE COMMON PART
        TestUtil.registerProcedure(db, Mongo.class, Graphs.class);



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
        BOUGHT_REFS = productReferences.stream().map(ObjectId::toString).collect(Collectors.toList());

        personCollection.insertOne(new Document(map("name", "Ajeje Brazorf",
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
        barCollection.insertOne(new Document(map("name", nameAsObjectId, "age", 54)));


        mongoClient.close();
    }

    private static Map<String, Object> getParams(String suffix) {
        return map("uri", URI + suffix);
    }

    @Test(expected = QueryExecutionException.class)
    public void shouldFailIfUriHasNonRootUserNotAuthorizedInDb() {
        try {
            testCall(db, "CALL apoc.mongo.find($uri,{query: {age: 54}})",
                    map("uri", String.format("mongodb://user:secret@%s:%s/foo.bar?authSource=admin", mongo.getContainerIpAddress(), mongo.getMappedPort(MONGO_DEFAULT_PORT))),
                    row -> {});
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("not authorized on foo to execute command"));
            throw e;
        }
    }

    @Test
    public void shouldNotFailIfUriHasNonRootUserNotAuthorizedInDb() {
        testCall(db, "CALL apoc.mongo.find($uri,{query: {age: 54}})",
                map("uri", String.format("mongodb://admin:pass@%s:%s/foo.bar?authSource=admin", mongo.getContainerIpAddress(), mongo.getMappedPort(MONGO_DEFAULT_PORT))),
                row -> {
                    final Map<String, Object> value = (Map<String, Object>) row.get("value");
                    assertEquals(nameAsObjectId.toString(), value.get("name"));
                    assertEquals(54L, value.get("age"));
                });
    }

    // todo - test connection without username e password --> che succede
    @Test(expected = QueryExecutionException.class)
    public void shouldFailIfUriHasNotCredentials() {

        final String bytes = Base64.getEncoder().encodeToString("fooBar".getBytes());
        try {
            testCall(db, "CALL apoc.mongo.get($uri, {query: {binary: {`$binary`: $bytes, `$subType`: '00'}}})",
                    map("uri", String.format("mongodb://%s:%s/test.test?authSource=admin", mongo.getContainerIpAddress(), mongo.getMappedPort(MONGO_DEFAULT_PORT)), "bytes", bytes),
                    row -> {});
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("not authorized on test to execute command"));
            throw e;
        }
    }

    @Test(expected = QueryExecutionException.class)
    public void shouldFailIfUriHasNotDatabase() {
        try {
            testCall(db, "CALL apoc.mongo.first($uri,{name:'testDocument'})",
                    map("uri", String.format("mongodb://admin:pass@%s:%s/?authSource=admin", mongo.getContainerIpAddress(), mongo.getMappedPort(MONGO_DEFAULT_PORT))),
                    r -> {});
        } catch (Exception e) {
            assertTrue(e.getMessage().contains(String.format(ERROR_MESSAGE, "db")));
            throw e;
        }
    }

    @Test(expected = QueryExecutionException.class)
    public void shouldFailIfUriHasNotCollection() {
        try {
            testCall(db, "CALL apoc.mongo.first($uri,{name:'testDocument'})",
                    map("uri", String.format("mongodb://admin:pass@%s:%s/test?authSource=admin", mongo.getContainerIpAddress(), mongo.getMappedPort(MONGO_DEFAULT_PORT))),
                    r -> {});
        } catch (Exception e) {
            assertTrue(e.getMessage().contains(String.format(ERROR_MESSAGE, "collection")));
            throw e;
        }
    }

    @Test
    public void objectIdAsString() {
        testResult(db, "CALL apoc.mongo.get($uri, {objectIdAsMap: false})", getParams("/test.person?authSource=admin"), res -> {
            // todo
        });
    }

    @Test
    public void testWithSkip() {
        // todo - variabile statica con questo...
        testResult(db, "CALL apoc.mongo.get($uri, {query: {expr: {`$regex`: 'foo*', `$options`: ''} } , skip: 1})", getParams("/test.person?authSource=admin"), res -> {
            final ResourceIterator<Map<String, Object>> values = res.columnAs("value");
            assertionsPersonJohn(values.next());
            assertionsPersonJack(values.next());
            assertFalse(values.hasNext());
        });
    }

    @Test
    public void testWithLimit() {
        testResult(db, "CALL apoc.mongo.get($uri, {query: {expr: {`$regex`: 'foo*', `$options`: ''}} , limit: 2})", getParams("/test.person?authSource=admin"), res -> {
            final ResourceIterator<Map<String, Object>> values = res.columnAs("value");
            assertionsPersonAl(values.next());
            assertionsPersonJohn(values.next());
            assertFalse(values.hasNext());
        });
    }

    @Test
    public void testWithSkipAndLimit() {
        testResult(db, "CALL apoc.mongo.get($uri, {query: {expr: {`$regex`: 'foo*', `$options`: ''}} , skip: 1, limit: 1})", getParams("/test.person?authSource=admin"), res -> {
            final ResourceIterator<Map<String, Object>> value = res.columnAs("value");
            assertionsPersonJohn(value.next());
            assertFalse(value.hasNext());
        });
    }

    @Test
    public void testFindSort() throws Exception {
        testResult(db, "CALL apoc.mongo.find($uri,{query: {expr: {`$regex`: 'foo*', `$options`: ''}}, sort: {name: -1}  })", getParams("/test.person?authSource=admin"), res -> {
            final ResourceIterator<Map<String, Object>> value = res.columnAs("value");
            assertionsPersonJohn(value.next());
            assertionsPersonJack(value.next());
            assertionsPersonAl(value.next());
            assertFalse(value.hasNext());
        });
    }

    @Test
    public void testFindWithProject() {
        testResult(db, "CALL apoc.mongo.find($uri,{query: {expr: {`$regex`: 'foo*', `$options`: ''}}, project: {age: 1}  })", getParams("/test.person?authSource=admin"), res -> {
            final ResourceIterator<Map<String, Object>> value = res.columnAs("value");
            final Set<String> expectedKeySet = Set.of("_id", "age");
            final Map<String, Object> first = value.next();
            assertEquals(expectedKeySet, first.keySet());
            assertEquals(25L, first.get("age"));
            assertEquals(SET_OBJECT_ID_MAP, ((Map<String, Object>) first.get("_id")).keySet());
            final Map<String, Object> second = value.next();
            assertEquals(expectedKeySet, second.keySet());
            assertEquals(45L, second.get("age"));
            assertEquals(SET_OBJECT_ID_MAP, ((Map<String, Object>) second.get("_id")).keySet());
            final Map<String, Object> third = value.next();
            assertEquals(expectedKeySet, third.keySet());
            assertEquals(54L, third.get("age"));
            assertEquals(SET_OBJECT_ID_MAP, ((Map<String, Object>) third.get("_id")).keySet());
            assertFalse(value.hasNext());
        });
    }

    @Test
    public void testFindManyConfigs() {
        testResult(db, "CALL apoc.mongo.find($uri,{query: {expr: {`$regex`: 'foo*', `$options`: ''}}, project: {age: 1}  })", getParams("/test.person?authSource=admin"), res -> {
            // TODO
        });
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
    public void testFirstWithoutFilterQuery() {
        testCall(db, "CALL apoc.mongo.first($uri)", getParams("/test.person?authSource=admin"), r -> {
            Map<String, Object> doc = (Map<String, Object>) r.get("value");
            assertEquals(Arrays.asList(12.345, 67.890), doc.get("coordinates"));
            assertEquals(LocalDateTime.of(1935,10,11, 0, 0), doc.get("born"));
            assertEquals("Ajeje Brazorf", doc.get("name"));
            assertEquals(BOUGHT_REFS, doc.get("bought"));
        });
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
        testResult(db, "CALL apoc.mongo.find($uri)", getParams("/test.test?authSource=admin"), this::assertResult);
    }

    @Test
    public void testFindWithExtendedJson() throws Exception {
        final String bytes = Base64.getEncoder().encodeToString("fooBar".getBytes());
        testResult(db, "CALL apoc.mongo.find($uri,{query: {binary: {`$binary`: $bytes, `$subType`: '00'}}})",
                map("uri", String.format("mongodb://user:secret@%s:%s/test.person?authSource=admin", mongo.getContainerIpAddress(), mongo.getMappedPort(MONGO_DEFAULT_PORT)), "bytes", bytes),

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
    public void testCountWithExtendedJson() throws Exception {
        final String bytes = Base64.getEncoder().encodeToString("fooBar".getBytes());
        testCall(db, "CALL apoc.mongo.count($uri,{query: {binary: {`$binary`: 'Zm9vQmFy', `$subType`: '00'}, int64: {`$numberLong`: '29'}}})",
                map("uri", URI + "/test.person?authSource=admin", "bytes", bytes),
                r -> assertEquals(2L, r.get("value")));
    }

    @Test
    public void testCountAll() throws Exception {
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

        // reset property as previously
        testCall(db, "CALL apoc.mongo.update($uri,{foo: {`$oid`: '57e193d7a9cc81b4027499c4'}},{`$set`:{code: {`$code`: 'function() {}'}}})",
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
            fail("Should fail because of BsonRegularExpression");
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

        testCall(db, "CALL apoc.mongo.delete($uri, {foo: {`$regex`: 'pattern', `$options`: ''}})", getParams("/test.person?authSource=admin"), r -> {
            long affected = (long) r.get("value");
            assertEquals(1L, affected);
        });
    }

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

    @Test(expected = QueryExecutionException.class)
    public void testInsertFailsDupKey() {
        try {
            testResult(db, "CALL apoc.mongo.insert($uri,[{foo:'bar', _id: 1}, {foo:'bar', _id: 1}])", getParams("/test.test?authSource=admin"), (r) -> {
                assertFalse("should be empty", r.hasNext());
            });
        } catch (QueryExecutionException e) {
            assertTrue(e.getMessage().contains("E11000 duplicate key error collection"));
            throw e;
        }
    }

    @Test
    public void shouldInsertDataIntoNeo4jWithFromDocument() throws Exception {
        Date date = DateUtils.parseDate("11-10-1935", "dd-MM-yyyy");
        testResult(db, "CALL apoc.mongo.first($uri, {extractReferences: true}) YIELD value " +
                        "CALL apoc.graph.fromDocument(value, $fromDocConfig) YIELD graph AS g1 " +
                        "RETURN g1",
                map("uri", URI + "/test.person?authSource=admin",
                        "compatibleValues", true,
                        "extractReferences", true,
                        "fromDocConfig", map("write", true, "skipValidation", true, "mappings", map("$", "Person:Customer{!name,bought,coordinates,born}", "$.bought", "Product{!name,price,tags}"))),
                r -> assertionsInsertDataWithFromDocument(date, r));
    }

    private void assertionsPersonAl(Map<String, Object> second) {
        assertEquals(25L, second.get("age"));
        assertEquals(LocalDateTime.ofInstant(Instant.ofEpochMilli(1234567), ZoneId.systemDefault()), second.get("date"));
        assertEquals(29L, second.get("int64"));
        assertEquals(12.34, second.get("double"));
        assertEquals(BOUGHT_REFS, second.get("bought"));
        assertEquals(fooAlAsObjectId.toString(), second.get("foo"));
        assertEquals("fooBar", new String((byte[]) second.get("binary")));
        assertEquals("Al", second.get("name"));
        assertEquals("foo*", second.get("expr"));
        assertEquals(SET_OBJECT_ID_MAP, ((Map<String, Object>) second.get("_id")).keySet());
    }

    private void assertionsPersonJack(Map<String, Object> value) {
        assertEquals(54L, value.get("age"));
        assertEquals("Jack", value.get("name"));
        assertEquals("MaxKey", value.get("maxKey"));
        assertEquals("MinKey", value.get("minKey"));
        assertEquals(BOUGHT_REFS, value.get("bought"));
        assertEquals("foo*", value.get("expr"));
        assertEquals(fooJackAsObjectId.toString(), value.get("foo"));
        assertEquals(SET_OBJECT_ID_MAP, ((Map<String, Object>) value.get("_id")).keySet());
    }

    private void assertionsPersonJohn(Map<String, Object> value) {
        assertEquals(45L, value.get("age"));
        assertEquals("John", value.get("name"));
        assertEquals("function() {}", value.get("code"));
        assertEquals("function() {}", value.get("codeWithScope"));
        assertEquals("x", value.get("sym"));
        assertEquals(BOUGHT_REFS, value.get("bought"));
        assertEquals("MaxKey", value.get("maxKey"));
        assertEquals("MinKey", value.get("minKey"));
        assertEquals("fooBar", new String((byte[]) value.get("binary")));
        assertEquals("foo*", value.get("expr"));
        assertEquals(123L, value.get("time"));
        assertEquals(fooJohnAsObjectId.toString(), value.get("foo"));
        assertEquals(29L, value.get("int64"));
        assertEquals(SET_OBJECT_ID_MAP, ((Map<String, Object>) value.get("_id")).keySet());
    }
}
