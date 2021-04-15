package apoc.mongodb;

import apoc.graph.Graphs;
import apoc.util.TestUtil;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.commons.collections.IteratorUtils;
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

//    @ClassRule
//    public static DbmsRule db = new ImpermanentDbmsRule();

    private static MongoCollection<Document> barCollection;

    private static String URI = null;
    private static List<String> BOUGHT_REFS;
    private static final Set<String> SET_OBJECT_ID_MAP = Set.of("date", "machineIdentifier", "processIdentifier", "counter", "time", "timestamp", "timeSecond");

    private static ObjectId fooAlAsObjectId = new ObjectId("77e193d7a9cc81b4027498b4");
    private static ObjectId fooJohnAsObjectId = new ObjectId("57e193d7a9cc81b4027499c4");
    private static ObjectId fooJackAsObjectId = new ObjectId("67e193d7a9cc81b4027518b4");
    private static ObjectId idAlAsObjectId = new ObjectId("97e193d7a9cc81b4027519b4");
    private static ObjectId idJackAsObjectId = new ObjectId("97e193d7a9cc81b4027518b4");
    private static ObjectId idJohnAsObjectId = new ObjectId("07e193d7a9cc81b4027488c4");

    @BeforeClass
    public static void setUp() throws Exception {
        createContainer(true);

        // readWrite user creation
        mongo.execInContainer("mongo", "admin", "--eval", "db.auth('admin', 'pass'); db.createUser({user: 'user', pwd: 'secret',roles: [{role: 'readWrite', db: 'test'}]});");

        final String host = mongo.getHost();
        final Integer port = mongo.getMappedPort(MONGO_DEFAULT_PORT);
        final String format = String.format("mongodb://admin:pass@%s:%s", host, port);
        MongoClientURI uri = new MongoClientURI(format);
        try(MongoClient mongoClient = new MongoClient(uri)) {
            URI = String.format("mongodb://admin:pass@%s:%s", mongo.getContainerIpAddress(), mongo.getMappedPort(MONGO_DEFAULT_PORT));

            fillDb(mongoClient);

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
            personCollection.insertOne(new Document(map("baz", "baa", "age", 200)));
            personCollection.insertOne(new Document(map("baz", "baa", "age", 1)));

            BOUGHT_REFS = productReferences.stream().map(ObjectId::toString).collect(Collectors.toList());
            MongoDatabase databaseFoo = mongoClient.getDatabase("foo");
            barCollection = databaseFoo.getCollection("bar");
            barCollection.insertOne(new Document(map("name", nameAsObjectId, "age", 54)));
        }
    }

    private static Map<String, Object> getParams(String suffix) {
        return map("uri", URI + suffix);
    }

    // - failing tests
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

    @Test(expected = QueryExecutionException.class)
    public void shouldFailIfUriHasNotCredentials() {
        try {
            testCall(db, "CALL apoc.mongo.get($uri, {query: {foo: 'bar' }})",
                    map("uri", String.format("mongodb://%s:%s/test.test?authSource=admin", mongo.getContainerIpAddress(), mongo.getMappedPort(MONGO_DEFAULT_PORT))),
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
    public void objectIdAsString() {
        final String bytes = Base64.getEncoder().encodeToString("fooBar".getBytes());
        testResult(db, "CALL apoc.mongo.get($uri, {query: {binary: {`$binary`: $bytes, `$subType`: '00'}}, objectIdAsMap: false})",
                map("uri", URI + "/test.person?authSource=admin", "bytes", bytes),
                res -> {
            final ResourceIterator<Map<String, Object>> values = res.columnAs("value");
            assertionsPersonAl(values.next(), false);
            assertionsPersonJohn(values.next(), false);
            assertFalse(values.hasNext());
        });
    }

    @Test
    public void testWithSkip() {
        // todo - variabile statica con questo...
        testResult(db, "CALL apoc.mongo.get($uri, {query: {expr: {`$regex`: 'foo*', `$options`: ''} } , skip: 1})", getParams("/test.person?authSource=admin"), res -> {
            final ResourceIterator<Map<String, Object>> values = res.columnAs("value");
            assertionsPersonJohn(values.next(), true);
            assertionsPersonJack(values.next(), true);
            assertFalse(values.hasNext());
        });
    }

    @Test
    public void testWithLimit() {
        testResult(db, "CALL apoc.mongo.get($uri, {query: {expr: {`$regex`: 'foo*', `$options`: ''}} , limit: 2})", getParams("/test.person?authSource=admin"), res -> {
            final ResourceIterator<Map<String, Object>> values = res.columnAs("value");
            assertionsPersonAl(values.next(), true);
            assertionsPersonJohn(values.next(), true);
            assertFalse(values.hasNext());
        });
    }

    @Test
    public void testWithSkipAndLimit() {
        testResult(db, "CALL apoc.mongo.get($uri, {query: {expr: {`$regex`: 'foo*', `$options`: ''}} , skip: 1, limit: 1})", getParams("/test.person?authSource=admin"), res -> {
            final ResourceIterator<Map<String, Object>> value = res.columnAs("value");
            assertionsPersonJohn(value.next(), true);
            assertFalse(value.hasNext());
        });
    }

    @Test
    public void testFindSort() throws Exception {
        testResult(db, "CALL apoc.mongo.find($uri,{query: {expr: {`$regex`: 'foo*', `$options`: ''}}, sort: {name: -1}  })", getParams("/test.person?authSource=admin"), res -> {
            final ResourceIterator<Map<String, Object>> value = res.columnAs("value");
            assertionsPersonJohn(value.next(), true);
            assertionsPersonJack(value.next(), true);
            assertionsPersonAl(value.next(), true);
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
    public void testFindWithManyConfigs() {
        testResult(db, "CALL apoc.mongo.find($uri,{query: {baz: 'baa'}, project: {age: 1}, sort: {name: -1}, {objectIdAsMap: false}, {skip: 1}, {limit: 2}})", getParams("/test.person?authSource=admin"), res -> {
            // todo
            IteratorUtils.toList(res.columnAs("value"));
        });
    }

    @Test
    public void testFirstWithoutFilterQuery() {
        testCall(db, "CALL apoc.mongo.first($uri)", getParams("/test.person?authSource=admin"), r -> {
            Map<String, Object> doc = (Map<String, Object>) r.get("value");
            assertEquals(Arrays.asList(12.345, 67.890), doc.get("coordinates"));
            assertEquals(LocalDateTime.of(1935,10,11, 0, 0), doc.get("born"));
            assertEquals("Andrea Santurbano", doc.get("name"));
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

    @Test
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
    public void testCountWithComplexTypes() throws Exception {
        final String bytes = Base64.getEncoder().encodeToString("fooBar".getBytes());
        testCall(db, "CALL apoc.mongo.count($uri,{query: {binary: {`$binary`: 'Zm9vQmFy', `$subType`: '00'}, int64: {`$numberLong`: '29'}}})",
                map("uri", URI + "/test.person?authSource=admin", "bytes", bytes),
                r -> assertEquals(2L, r.get("value")));
    }

    @Test
    public void testUpdate() throws Exception {
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
    public void testInsert() throws Exception {
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
    public void testGetInsertAndDeleteWithObjectId() throws Exception {
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

    // -- tests similar to MongoDbTest.java to check consistency with old procedures
    @Test
    public void testFind() throws Exception {
        testResult(db, "CALL apoc.mongo.find($uri)", getParams("/test.test?authSource=admin"), MongoTest::assertResult);
    }

    @Test
    public void testGet()  {
        testResult(db, "CALL apoc.mongo.get($uri)", getParams("/test.test?authSource=admin"), MongoTest::assertResult);
    }

    @Test
    public void testCountAll() throws Exception {
        testCall(db, "CALL apoc.mongo.count($uri,{query: {name: 'testDocument'}})", getParams("/test.test?authSource=admin"),
                r -> assertEquals(NUM_OF_RECORDS, r.get("value")));
    }

    @Test(expected = QueryExecutionException.class)
    public void testInsertFailsWithDuplicateKey() {
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
                        "fromDocConfig", map("write", true, "skipValidation", true, "mappings",
                                map("$", "Person:Customer{!name,bought,coordinates,born}", "$.bought", "Product{!name,price,tags}"))),
                r -> assertionsInsertDataWithFromDocument(date, r));
    }


    private void assertionsPersonAl(Map<String, Object> value, boolean idAsMap) {
        assertEquals(25L, value.get("age"));
        assertEquals(LocalDateTime.ofInstant(Instant.ofEpochMilli(1234567), ZoneId.systemDefault()), value.get("date"));
        assertEquals(29L, value.get("int64"));
        assertEquals(12.34, value.get("double"));
        assertEquals(BOUGHT_REFS, value.get("bought"));
        assertEquals(fooAlAsObjectId.toString(), value.get("foo"));
        assertEquals("fooBar", new String((byte[]) value.get("binary")));
        assertEquals("Al", value.get("name"));
        assertEquals("foo*", value.get("expr"));
        if (idAsMap) {
            assertEquals(SET_OBJECT_ID_MAP, ((Map<String, Object>) value.get("_id")).keySet());
        } else {
            assertEquals(idAlAsObjectId.toString(), value.get("_id"));
        }
    }

    private void assertionsPersonJack(Map<String, Object> value, boolean idAsMap) {
        assertEquals(54L, value.get("age"));
        assertEquals("Jack", value.get("name"));
        assertEquals("MaxKey", value.get("maxKey"));
        assertEquals("MinKey", value.get("minKey"));
        assertEquals(BOUGHT_REFS, value.get("bought"));
        assertEquals("foo*", value.get("expr"));
        assertEquals(fooJackAsObjectId.toString(), value.get("foo"));
        if (idAsMap) {
            assertEquals(SET_OBJECT_ID_MAP, ((Map<String, Object>) value.get("_id")).keySet());
        } else {
            assertEquals(idJackAsObjectId.toString(), value.get("_id"));
        }
    }

    private void assertionsPersonJohn(Map<String, Object> value, boolean idAsMap) {
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
        if (idAsMap) {
            assertEquals(SET_OBJECT_ID_MAP, ((Map<String, Object>) value.get("_id")).keySet());
        } else {
            assertEquals(idJohnAsObjectId.toString(), value.get("_id"));
        }
    }
}
