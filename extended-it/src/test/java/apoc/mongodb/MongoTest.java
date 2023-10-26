package apoc.mongodb;

import apoc.graph.Graphs;
import apoc.util.TestUtil;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.commons.lang3.time.DateUtils;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
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
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Base64;
import java.util.Date;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import static apoc.mongodb.MongoDBColl.ERROR_MESSAGE;
import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testResult;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class MongoTest extends MongoTestBase {

    private static String PERSON_URI = null;
    private static String TEST_URI = null;

    private static ObjectId fooAlAsObjectId = new ObjectId("77e193d7a9cc81b4027498b4");
    private static ObjectId fooJohnAsObjectId = new ObjectId("57e193d7a9cc81b4027499c4");
    private static ObjectId fooJackAsObjectId = new ObjectId("67e193d7a9cc81b4027518b4");
    private static ObjectId idAlAsObjectId = new ObjectId("97e193d7a9cc81b4027519b4");
    private static ObjectId idJackAsObjectId = new ObjectId("97e193d7a9cc81b4027518b4");
    private static ObjectId idJohnAsObjectId = new ObjectId("07e193d7a9cc81b4027488c4");

    @BeforeClass
    public static void setUp() throws Exception {
        beforeClassCommon(MongoVersion.LATEST);
    }

    static void beforeClassCommon(MongoVersion mongoVersion) throws Exception {
        createContainer(true, mongoVersion);
        
        TestUtil.registerProcedure(db, Mongo.class, Graphs.class);

        // readWrite user creation
        mongo.execInContainer(mongoVersion.shell, "admin", "--eval", "db.auth('admin', 'pass'); db.createUser({user: 'user', pwd: 'secret',roles: [{role: 'readWrite', db: 'test'}]});");

        final String host = mongo.getHost();
        final Integer port = mongo.getMappedPort(MONGO_DEFAULT_PORT);
        final String format = String.format("mongodb://admin:pass@%s:%s", host, port);
        try (MongoClient mongoClient = MongoClients.create(format)) {
            String uriPrefix = String.format("mongodb://admin:pass@%s:%s", mongo.getContainerIpAddress(), mongo.getMappedPort(MONGO_DEFAULT_PORT));
            PERSON_URI = uriPrefix + "/test.person?authSource=admin";
            TEST_URI = uriPrefix + "/test.test?authSource=admin";

            fillDb(mongoClient);

            personCollection.insertOne(new Document(map("_id", idAlAsObjectId,
                    "name", "Al",
                    "age", 25,
                    "baz", "baa",
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
                    "baz", "baa",
                    "foo", fooJohnAsObjectId,
                    "code", new Code("function() {}"),
                    "sym", new Symbol("x"),
                    "codeWithScope", new CodeWithScope("function() {}", new Document("k", "v")),
                    "int64", new BsonInt64(29L),
                    "int32", new BsonInt32(29),
                    "bsonDouble", new BsonDouble(6.78),
                    "time", new BsonTimestamp(123, 4),
                    "minKey", new MinKey(),
                    "maxKey", new MaxKey(),
                    "expr", new BsonRegularExpression("foo*"),
                    "binary", new Binary("fooBar".getBytes()),
                    "bought", productReferences)));
            personCollection.insertOne(new Document(map("_id", idJackAsObjectId,
                    "name", "Jack",
                    "age", 54,
                    "baz", "baa",
                    "foo", fooJackAsObjectId,
                    "expr", new BsonRegularExpression("foo*"),
                    "minKey", new MinKey(),
                    "maxKey", new MaxKey(),
                    "bought", productReferences)));
            personCollection.insertOne(new Document(map("name", "vvv", "baz", "baa", "foo", "custom", "age", 200)));
            personCollection.insertOne(new Document(map("name", "zzz", "baz", "baa", "foo", "custom","age", 1)));
            personCollection.insertOne(new Document(map("name", "another", "foo", "custom","age", 666)));
            personCollection.insertOne(new Document(map("name", "one", "foo", "custom", "age", 777)));
            personCollection.insertOne(new Document(map("name", "two", "foo", "custom", "age", 11)));

            MongoDatabase databaseFoo = mongoClient.getDatabase("foo");
            MongoCollection<Document> barCollection = databaseFoo.getCollection("bar");
            barCollection.insertOne(new Document(map("name", nameAsObjectId, "age", 54)));
        }
    }

    // - failing tests
    @Test(expected = QueryExecutionException.class)
    public void shouldFailIfUriHasNonRootUserNotAuthorizedInDb() {
        try {
            testCall(db, "CALL apoc.mongo.find($uri,{query: {age: 54}})",
                    map("uri", String.format("mongodb://user:secret@%s:%s/foo.bar?authSource=admin", mongo.getContainerIpAddress(), mongo.getMappedPort(MONGO_DEFAULT_PORT))),
                    row -> {
                    });
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("not authorized on foo to execute command"));
            throw e;
        }
    }

    @Test
    public void shouldNotFailIfUriHasRootUser() {
        testCall(db, "CALL apoc.mongo.find($uri, {age: 54})",
                map("uri", String.format("mongodb://admin:pass@%s:%s/foo.bar?authSource=admin", mongo.getContainerIpAddress(), mongo.getMappedPort(MONGO_DEFAULT_PORT))),
                row -> {
                    final Map<String, Object> value = (Map<String, Object>) row.get("value");
                    Assert.assertEquals(nameAsObjectId.toString(), value.get("name"));
                    assertEquals(54L, value.get("age"));
                });
    }

    @Test(expected = QueryExecutionException.class)
    public void shouldFailIfUriHasNotCredentials() {
        try {
            testCall(db, "CALL apoc.mongo.find($uri, {query: {foo: 'bar' }})",
                    map("uri", String.format("mongodb://%s:%s/test.test?authSource=admin", mongo.getContainerIpAddress(), mongo.getMappedPort(MONGO_DEFAULT_PORT))),
                    row -> {});
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("Command failed with error 13"));
            throw e;
        }
    }

    @Test(expected = QueryExecutionException.class)
    public void shouldFailIfUriHasNotDatabase() {
        try {
            testCall(db, "CALL apoc.mongo.find($uri,{name:'testDocument'})",
                    map("uri", String.format("mongodb://admin:pass@%s:%s/?authSource=admin", mongo.getContainerIpAddress(), mongo.getMappedPort(MONGO_DEFAULT_PORT))),
                    r -> {});
        } catch (Exception e) {
            assertTrue(e.getMessage().contains(String.format(ERROR_MESSAGE, "db")));
            throw e;
        }
    }

    @Test(expected = QueryExecutionException.class)
    public void shouldFailIfNeitherUriNorConfigHasCollection() {
        try {
            testCall(db, "CALL apoc.mongo.count($uri,{name:'testDocument'})",
                    map("uri", String.format("mongodb://admin:pass@%s:%s/test?authSource=admin", mongo.getContainerIpAddress(), mongo.getMappedPort(MONGO_DEFAULT_PORT))),
                    r -> {});
        } catch (Exception e) {
            assertTrue(e.getMessage().contains(String.format(ERROR_MESSAGE, "collection")));
            throw e;
        }
    }

    @Test
    public void shouldNotFailsIfUriHasNotCollectionNameButIsPresentInConfig() {
        testCall(db, "CALL apoc.mongo.count($uri, {name:'testDocument'}, {collection: 'test'})", 
                map("uri", String.format("mongodb://admin:pass@%s:%s/test?authSource=admin", mongo.getContainerIpAddress(), mongo.getMappedPort(MONGO_DEFAULT_PORT))),
                r -> assertEquals(NUM_OF_RECORDS, r.get("value")));
    }

    @Test
    public void objectIdAsString() {
        final String bytes = Base64.getEncoder().encodeToString("fooBar".getBytes());
        testResult(db, "CALL apoc.mongo.find($uri, {binary: {`$binary`: $bytes, `$subType`: '00'}}, {objectIdAsMap: false})",
                map("uri", PERSON_URI, "bytes", bytes),
                res -> {
                    final ResourceIterator<Map<String, Object>> values = res.columnAs("value");
                    assertionsPersonAl(values.next(), false, false);
                    assertionsPersonJohn(values.next(), false, false);
                    assertFalse(values.hasNext());
                });
    }

    @Test
    public void countWithStringOrMapBinaryQuery() {
        String bytesEncoded = Base64.getEncoder().encodeToString("fooBar".getBytes());
        Map<String, Object> params = map("uri", PERSON_URI);
        Consumer<Map<String, Object>> consumer = r -> assertEquals(2L, r.get("value"));
        
        // string queries (with respectively double and single quotes)
        testCall(db, "CALL apoc.mongo.count($uri, \"{'binary':{'$binary':'%s','$type':'00'}}\")".formatted(bytesEncoded),
                params, consumer);
        
        testCall(db, "CALL apoc.mongo.count($uri, '{\"binary\":{\"$binary\":\"%s\",\"$type\":\"00\"}}')".formatted(bytesEncoded),
                params, consumer);

        // map query
        testCall(db, "CALL apoc.mongo.count($uri, {binary: {`$binary`: '%s', `$type`: '00'}})".formatted(bytesEncoded),
                params, consumer);

        // legacy queries (used by mongodb-driver:3.2.2)
        // i.e. with `$subType` instead of `$type`
        
        // string queries (with respectively double and single quotes) 
        testCall(db, "CALL apoc.mongo.count($uri, \"{'binary':{'$binary':'%s','$subType':'00'}}\")".formatted(bytesEncoded),
                params, consumer);

        testCall(db, "CALL apoc.mongo.count($uri, '{\"binary\":{\"$binary\":\"%s\",\"$subType\":\"00\"}}')".formatted(bytesEncoded),
                params, consumer);
        
        // map query
        testCall(db, "CALL apoc.mongo.count($uri, {binary: {`$binary`: '%s', `$subType`: '00'}})".formatted(bytesEncoded),
                params, consumer);
    }

    @Test
    public void testWithSkip() {
        testResult(db, "CALL apoc.mongo.find($uri, {expr: {`$regex`: 'foo*', `$options`: ''}}, {skip: 1})", map("uri", PERSON_URI), res -> {
            final ResourceIterator<Map<String, Object>> values = res.columnAs("value");
            assertionsPersonJohn(values.next(), true, false);
            assertionsPersonJack(values.next(), true, false);
            assertFalse(values.hasNext());
        });
    }

    @Test
    public void testWithLimit() {
        testResult(db, "CALL apoc.mongo.find($uri, {expr: {`$regex`: 'foo*', `$options`: ''}}, {limit: 2})", map("uri", PERSON_URI), res -> {
            final ResourceIterator<Map<String, Object>> values = res.columnAs("value");
            assertionsPersonAl(values.next(), true, false);
            assertionsPersonJohn(values.next(), true, false);
            assertFalse(values.hasNext());
        });
    }

    @Test
    public void testWithSkipAndLimit() {
        testResult(db, "CALL apoc.mongo.find($uri, {expr: {`$regex`: 'foo*', `$options`: ''}}, {skip: 1, limit: 1})", map("uri", PERSON_URI), res -> {
            final ResourceIterator<Map<String, Object>> value = res.columnAs("value");
            assertionsPersonJohn(value.next(), true, false);
            assertFalse(value.hasNext());
        });
    }

    @Test
    public void testAggregation() {
        testResult(db, "CALL apoc.mongo.aggregate($uri, [{`$match`: {foo: 'custom'}}, {`$sort`: {name: -1}}, {`$skip`: 1}, {`$limit`: 2}, {`$set`: {aggrField: 'Y'} }], $conf)", 
                map("uri", PERSON_URI, "conf", map("objectIdAsMap", false)), res -> {
            final ResourceIterator<Map<String, Object>> value = res.columnAs("value");
            final Map<String, Object> first = value.next();
            assertEquals("custom", first.get("foo"));
            assertEquals("vvv", first.get("name"));
            assertEquals("baa", first.get("baz"));
            assertEquals("Y", first.get("aggrField"));
            assertEquals(200L, first.get("age"));
            assertTrue(first.get("_id") instanceof String);
            final Map<String, Object> second = value.next();
            assertEquals("custom", second.get("foo"));
            assertEquals("two", second.get("name"));
            assertEquals("Y", second.get("aggrField"));
            assertEquals(11L, second.get("age"));
            assertTrue(second.get("_id") instanceof String);
            assertFalse(value.hasNext());
        });
    }

    @Test
    public void testFindSort() {
        testResult(db, "CALL apoc.mongo.find($uri, {expr: {`$regex`: 'foo*', `$options`: ''}}, {sort: {name: -1} })", map("uri", PERSON_URI), res -> {
            final ResourceIterator<Map<String, Object>> value = res.columnAs("value");
            assertionsPersonJohn(value.next(), true, false);
            assertionsPersonJack(value.next(), true, false);
            assertionsPersonAl(value.next(), true, false);
            assertFalse(value.hasNext());
        });
    }

    @Test
    public void testFindWithExtractReference() {
        testResult(db, "CALL apoc.mongo.find($uri, {expr: {`$regex`: 'foo*', `$options`: ''}}, {extractReferences: true})", map("uri", PERSON_URI), res -> {
            final ResourceIterator<Map<String, Object>> value = res.columnAs("value");
            assertionsPersonAl(value.next(), true, true);
            assertionsPersonJohn(value.next(), true, true);
            assertionsPersonJack(value.next(), true, true);
            assertFalse(value.hasNext());
        });
    }

    @Test
    public void testFindWithProject() {
        testResult(db, "CALL apoc.mongo.find($uri, {expr: {`$regex`: 'foo*', `$options`: ''}}, {project: {age: 1}  })", map("uri", PERSON_URI), res -> {
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
    public void testFindWithManyConfigs() {
        testResult(db, "CALL apoc.mongo.find($uri, {baz: 'baa'}, {project: {age: 1}, sort: {name: -1}, objectIdAsMap: false, skip: 1, limit: 3})",
                map("uri", PERSON_URI), this::assertionsWithManyConfig);
    }

    @Test
    public void testFindWithStringifiedParameters() {
        testResult(db, "CALL apoc.mongo.find($uri, '{baz: \"baa\"}', {project: '{age: 1}', sort: '{name: -1}', objectIdAsMap: false, skip: 1, limit: 3})",
                map("uri", PERSON_URI), this::assertionsWithManyConfig);
    }

    private void assertionsWithManyConfig(Result res) {
        final ResourceIterator<Map<String, Object>> value = res.columnAs("value");
        final Set<String> expectedKeySet = Set.of("_id", "age");
        final Map<String, Object> first = value.next();
        assertEquals(expectedKeySet, first.keySet());
        assertEquals(200L, first.get("age"));
        assertTrue(first.get("_id") instanceof String);
        final Map<String, Object> second = value.next();
        assertEquals(expectedKeySet, second.keySet());
        assertEquals(45L, second.get("age"));
        Assert.assertEquals(idJohnAsObjectId.toString(), second.get("_id"));
        final Map<String, Object> third = value.next();
        assertEquals(expectedKeySet, third.keySet());
        assertEquals(54L, third.get("age"));
        Assert.assertEquals(idJohnAsObjectId.toString(), second.get("_id"));
        assertFalse(value.hasNext());
    }

    @Test
    public void testFindWithExtractReferencesTrue() {
        testCall(db, "CALL apoc.mongo.find($uri, {`_id`: {`$oid`: '97e193d7a9cc81b4027519b4'}}, {extractReferences: true, objectIdAsMap: false})",
                map("uri", PERSON_URI), r -> {
                    Map<String, Object> doc = (Map<String, Object>) r.get("value");
                    assertionsPersonAl(doc, false, true);
        });
    }

    @Test
    public void testCountWithComplexTypes() {
        final String bytes = Base64.getEncoder().encodeToString("fooBar".getBytes());
        testCall(db, "CALL apoc.mongo.count($uri, {binary: {`$binary`: 'Zm9vQmFy', `$type`: '00'}, int64: {`$numberLong`: '29'}})",
                map("uri", PERSON_URI, "bytes", bytes),
                r -> assertEquals(2L, r.get("value")));
    }

    @Test
    public void testUpdate() {
        testCall(db, "CALL apoc.mongo.update($uri, {foo: {`$oid`: '57e193d7a9cc81b4027499c4'}}, {`$set`:{code: {`$code`: 'void 0'}}})",
                map("uri", PERSON_URI), r -> {
                    long affected = (long) r.get("value");
                    assertEquals(1, affected);
        });

        // reset property as previously
        testCall(db, "CALL apoc.mongo.update($uri, {foo: {`$oid`: '57e193d7a9cc81b4027499c4'}},{`$set`:{code: {`$code`: 'function() {}'}}})",
                map("uri", PERSON_URI), r -> {
                    long affected = (long) r.get("value");
                    assertEquals(1, affected);
        });
    }

    @Test
    public void testInsert() {
        testResult(db, "CALL apoc.mongo.insert($uri, [{secondId: {`$oid`: '507f191e811c19729de860ea'}, baz: 1}, {secondId: {`$oid`: '507f191e821c19729de860ef'}, baz: 1}])",
                map("uri", PERSON_URI), (r) -> assertFalse("should be empty", r.hasNext()));

        testCall(db, "CALL apoc.mongo.count($uri, {baz:1})", map("uri", PERSON_URI), r -> {
            long affected = (long) r.get("value");
            assertEquals(2L, affected);
        });
    }

    @Test
    public void testInsertWithRegex() {
        testResult(db, "CALL apoc.mongo.insert($uri,[{foo:{`$regex`: 'pattern', `$options`: ''}, myId: {`$oid`: '507f191e811c19729de960ea'}}])",
                map("uri", PERSON_URI), (r) -> assertFalse("should be empty", r.hasNext()));

        testCall(db, "CALL apoc.mongo.find($uri, {foo: {`$regex`: 'pattern', `$options`: ''}})", map("uri", PERSON_URI), r -> {
            Map<String, Object> value = (Map<String, Object>) r.get("value");
            assertEquals("pattern", value.get("foo"));
            assertTrue(value.get("_id") instanceof Map);
            assertEquals(SET_OBJECT_ID_MAP, ((Map<String, Object>) value.get("_id")).keySet());
            assertEquals("507f191e811c19729de960ea", value.get("myId"));
        });

        testCall(db, "CALL apoc.mongo.delete($uri, {foo: {`$regex`: 'pattern', `$options`: ''}})", map("uri", PERSON_URI), r -> {
            long affected = (long) r.get("value");
            assertEquals(1L, affected);
        });
    }

    @Test
    public void testGetInsertAndDeleteWithObjectId() {
        testResult(db, "CALL apoc.mongo.insert($uri,[{foo:'bar', myId: {`$oid`: '507f191e811c19729de960ea'}}])", map("uri", PERSON_URI), (r) -> {
            assertFalse("should be empty", r.hasNext());
        });
        testCall(db, "CALL apoc.mongo.delete($uri,{foo: 'bar', myId: {`$oid`: '507f191e811c19729de960ea'}})", map("uri", PERSON_URI), r -> {
            long affected = (long) r.get("value");
            assertEquals(1L, affected);
        });
        testResult(db, "CALL apoc.mongo.find($uri,{query: {foo:'bar', myId: {`$oid`: '507f191e811c19729de960ea'}}})", map("uri", PERSON_URI), r -> {
            assertFalse("should be empty", r.hasNext());
        });
    }

    // -- tests similar to MongoDbTest.java to check consistency with old procedures
    @Test
    public void testFind() {
        testResult(db, "CALL apoc.mongo.find($uri)", map("uri", TEST_URI), MongoTest::assertResult);
    }

    @Test
    public void testCountAll() {
        testCall(db, "CALL apoc.mongo.count($uri, {name: 'testDocument'})", map("uri", TEST_URI),
                r -> assertEquals(NUM_OF_RECORDS, r.get("value")));
    }

    @Test(expected = QueryExecutionException.class)
    public void testInsertFailsWithDuplicateKey() {
        try {
            testResult(db, "CALL apoc.mongo.insert($uri, [{foo:'bar', _id: 1}, {foo:'bar', _id: 1}])", map("uri", TEST_URI), (r) -> {
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
        testResult(db, "CALL apoc.mongo.find($uri, {name: $name}, {extractReferences: true}) YIELD value " +
                        "CALL apoc.graph.fromDocument(value, $fromDocConfig) YIELD graph AS g1 " +
                        "RETURN g1",
                map("uri", PERSON_URI, "name", "Andrea Santurbano",
                        "fromDocConfig", map("write", true, "skipValidation", true, "mappings",
                                map("$", "Person:Customer{!name,bought,coordinates,born}", "$.bought", "Product{!name,price,tags}"))),
                r -> assertionsInsertDataWithFromDocument(date, r));
    }

    private void assertionsPersonAl(Map<String, Object> value, boolean idAsMap, boolean extractReferences) {
        assertEquals(25L, value.get("age"));
        assertEquals(LocalDateTime.ofInstant(Instant.ofEpochMilli(1234567), ZoneId.systemDefault()), value.get("date"));
        assertEquals(29L, value.get("int64"));
        assertEquals(12.34, value.get("double"));
        assertEquals("fooBar", new String((byte[]) value.get("binary")));
        assertEquals("Al", value.get("name"));
        assertEquals("foo*", value.get("expr"));
        assertEquals("baa", value.get("baz"));
        assertDocumentIdAndRefs(value, extractReferences, idAsMap, idAlAsObjectId);
    }

    private void assertionsPersonJack(Map<String, Object> value, boolean idAsMap, boolean extractReferences) {
        assertEquals(54L, value.get("age"));
        assertEquals("Jack", value.get("name"));
        assertEquals("MaxKey", value.get("maxKey"));
        assertEquals("MinKey", value.get("minKey"));
        assertEquals("foo*", value.get("expr"));
        assertEquals("baa", value.get("baz"));
        assertDocumentIdAndRefs(value, extractReferences, idAsMap, idJackAsObjectId);
    }

    private void assertionsPersonJohn(Map<String, Object> value, boolean idAsMap, boolean extractReferences) {
        assertEquals(45L, value.get("age"));
        assertEquals("John", value.get("name"));
        assertEquals("function() {}", value.get("code"));
        assertEquals("function() {}", value.get("codeWithScope"));
        assertEquals("x", value.get("sym"));
        assertEquals("MaxKey", value.get("maxKey"));
        assertEquals("MinKey", value.get("minKey"));
        assertEquals("fooBar", new String((byte[]) value.get("binary")));
        assertEquals("foo*", value.get("expr"));
        assertEquals(123L, value.get("time"));
        assertEquals(29L, value.get("int64"));
        assertEquals(29L, value.get("int32"));
        assertEquals(6.78D, value.get("bsonDouble"));
        assertEquals("baa", value.get("baz"));
        assertDocumentIdAndRefs(value, extractReferences, idAsMap, idJohnAsObjectId);
    }

    private void assertDocumentIdAndRefs(Map<String, Object> value, boolean extractReferences, boolean idAsMap, ObjectId idAlAsObjectId) {
        if (extractReferences) {
            assertBoughtReferences(value, idAsMap, true);
        } else {
            assertEquals(boughtListObjectIds, value.get("bought"));
        }
        if (idAsMap) {
            assertEquals(SET_OBJECT_ID_MAP, ((Map<String, Object>) value.get("_id")).keySet());
        } else {
            Assert.assertEquals(idAlAsObjectId.toString(), value.get("_id"));
        }
    }
}
