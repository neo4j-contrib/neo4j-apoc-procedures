package apoc.mongodb;

import apoc.graph.Graphs;
import apoc.util.MapUtil;
import apoc.util.TestUtil;
import apoc.util.UrlResolver;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import org.apache.commons.lang3.time.DateUtils;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Result;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.StreamSupport;

import static apoc.util.MapUtil.map;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * @author mh
 * @since 30.06.16
 */
public class MongoDBTest extends MongoTestBase {
    private static Map<String, Object> params;

    @BeforeClass
    public static void setUp() throws Exception {
        createContainer(false);
        MongoClient mongoClient = new MongoClient(mongo.getContainerIpAddress(), mongo.getMappedPort(MONGO_DEFAULT_PORT));
        HOST = String.format("mongodb://%s:%s", mongo.getContainerIpAddress(), mongo.getMappedPort(MONGO_DEFAULT_PORT));
        params = map("host", HOST, "db", "test", "collection", "test");

        fillDb(mongoClient);
        mongoClient.close();
    }

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void shouldExtractObjectIdsAsMaps() {
        boolean hasException = false;
        String url = new UrlResolver("mongodb", mongo.getContainerIpAddress(), mongo.getMappedPort(MONGO_DEFAULT_PORT))
                .getUrl("mongodb", mongo.getContainerIpAddress());
        try (MongoDBUtils.Coll coll = MongoDBUtils.Coll.Factory.create(url, "test", "person", false, true, false)) {
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
        try (MongoDBUtils.Coll coll = MongoDBUtils.Coll.Factory.create(url, "test", "person", false, false, false)) {
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
        try (MongoDBUtils.Coll coll = MongoDBUtils.Coll.Factory.create(url, "test", "test", true, false, true)) {
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
                r -> assertionsInsertDataWithFromDocument(date, r));
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
}
