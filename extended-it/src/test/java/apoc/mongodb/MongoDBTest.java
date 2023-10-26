package apoc.mongodb;

import apoc.graph.Graphs;
import apoc.util.MapUtil;
import apoc.util.TestUtil;
import apoc.util.UrlResolver;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.text.ParseException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

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

    private static String HOST = null;

    @BeforeClass
    public static void setUp() throws Exception {
        beforeClassCommon(MongoVersion.LATEST);
    }

    static void beforeClassCommon(MongoVersion mongoVersion) throws ParseException {
        createContainer(false, mongoVersion);
        HOST = String.format("mongodb://%s:%s", mongo.getContainerIpAddress(), mongo.getMappedPort(MONGO_DEFAULT_PORT));
        MongoClient mongoClient = MongoClients.create(HOST);

        fillDb(mongoClient);

        TestUtil.registerProcedure(db, MongoDB.class, Graphs.class);
        mongoClient.close();
    }

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void shouldExtractObjectIdsAsMaps() {
        boolean hasException = false;
        String url = new UrlResolver("mongodb", mongo.getContainerIpAddress(), mongo.getMappedPort(MONGO_DEFAULT_PORT))
                .getUrl("mongodb", mongo.getContainerIpAddress());
        try (MongoDbCollInterface coll = MongoDbCollInterface.Factory.create(url, "test", "person", false, true, false)) {
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
        try (MongoDbCollInterface coll = MongoDbCollInterface.Factory.create(url, "test", "person", false, false, false)) {
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
        try (MongoDbCollInterface coll = MongoDbCollInterface.Factory.create(url, "test", "test", true, false, true)) {
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
    public void testGetByObjectId() throws Exception {
        List<String> refsIds = productReferences.stream().map(ObjectId::toString).collect(Collectors.toList());
        TestUtil.testCall(db, "CALL apoc.mongodb.get.byObjectId($host,$db,$collection,$id)",
                map("host", HOST, "db", "test", "collection", "person", "id", idAsObjectId.toString()), r -> {
                    Map doc = (Map) r.get("value");
                    assertTrue(doc.get("_id") instanceof Map);
                    assertEquals(
                            Set.of("date", "timestamp"),
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
                            Set.of("date", "timestamp"),
                            ((Map<String, Object>) doc.get("_id")).keySet()
                    );
                    assertEquals(40L, doc.get("age"));
                    assertEquals(refsIds, doc.get("bought"));
        });
    }

    @Test
    public void testGetByObjectIdWithConfigs() throws Exception {
        TestUtil.testCall(db, "CALL apoc.mongodb.get.byObjectId($host, $db, $collection, $objectId, $config)",
                map("host", HOST, "db", "test", "collection", "person", "objectId", idAsObjectId.toString(),
                        "config", map("extractReferences", true, "objectIdAsMap", false, "compatibleValues", false)), r -> {
                    Map doc = (Map) r.get("value");
                    Assert.assertEquals(idAsObjectId.toString(), doc.get("_id"));
                    assertEquals(25, doc.get("age"));
                    assertEquals("Sherlock", doc.get("name"));
                    assertBoughtReferences(doc, false, false);
                });
    }
}
