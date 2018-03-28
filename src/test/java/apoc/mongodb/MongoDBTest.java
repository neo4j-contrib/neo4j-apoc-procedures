package apoc.mongodb;

import apoc.util.MapUtil;
import apoc.util.TestUtil;
import apoc.util.UrlResolver;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoTimeoutException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.Date;
import java.util.Map;

import static apoc.util.MapUtil.map;
import static org.junit.Assert.*;

/**
 * @author mh
 * @since 30.06.16
 */
public class MongoDBTest {
    private static GraphDatabaseService db;
    private static boolean mongoDBRunning = false;
    private static MongoCollection<Document> collection;
    private static MongoClient mongoClient;

    private static Date currentTime = new Date();

    private static long longValue = 10_000L;

    @BeforeClass
    public static void setUp() throws Exception {
        TestUtil.ignoreException(() -> {
            MongoClientOptions options = MongoClientOptions.builder()
                    .socketTimeout(100).serverSelectionTimeout(100)
                    .heartbeatConnectTimeout(100).heartbeatSocketTimeout(100).heartbeatFrequency(100).minHeartbeatFrequency(100)
                    .maxWaitTime(100).connectTimeout(100).build();
            mongoClient = new MongoClient("192.168.99.100", options);
            MongoDatabase database = mongoClient.getDatabase("test");
            collection = database.getCollection("test");
            collection.deleteMany(new Document());
            collection.insertOne(new Document(map("name", "testDocument", "date", currentTime, "longValue", longValue)));
            mongoDBRunning = true;
        }, MongoTimeoutException.class);
        if (mongoDBRunning) {
            db = new TestGraphDatabaseFactory()
                    .newImpermanentDatabaseBuilder()
                    .newGraphDatabase();
            TestUtil.registerProcedure(db, MongoDB.class);
        }
    }

    @AfterClass
    public static void tearDown() {
        if (db != null) db.shutdown();
    }

    @Test
    public void testObjectIdtoStringMapping() {
        Assume.assumeTrue(mongoDBRunning);
        TestUtil.ignoreException(() -> {
            String url = new UrlResolver("mongodb", "192.168.99.100", 27017).getUrl("mongodb", "192.168.99.100");
            MongoDB.Coll coll = MongoDB.Coll.Factory.create(url, "test", "test", false);

            Map<String, Object> document = coll.first(MapUtil.map("name", "testDocument"));
            assertTrue(document.get("_id") instanceof String);
        });
    }

    @Test
    public void testCompatibleValues() {
        Assume.assumeTrue(mongoDBRunning);
        TestUtil.ignoreException(() -> {
            String url = new UrlResolver("mongodb", "192.168.99.100", 27017).getUrl("mongodb", "192.168.99.100");
            MongoDB.Coll coll = MongoDB.Coll.Factory.create(url, "test", "test", true);

            Map<String, Object> document = coll.first(MapUtil.map("name", "testDocument"));
            assertNotNull(((Map<String, Object>) document.get("_id")).get("timestamp"));
            assertEquals(currentTime.getTime(), document.get("date"));
            assertEquals(longValue, document.get("longValue"));
        });
    }

    @Test
    public void testGet() throws Exception {
        Assume.assumeTrue(mongoDBRunning);
        TestUtil.ignoreException(() -> {
            TestUtil.testCall(db, "CALL apoc.mongodb.get('mongodb://192.168.99.100:27017','test','test',null)", r -> {
                Map doc = (Map) r.get("value");
                assertNotNull(doc.get("_id"));
                assertEquals("testDocument", doc.get("name"));
            });
        }, MongoTimeoutException.class);
    }

    @Test
    public void testGetCompatible() throws Exception {
        Assume.assumeTrue(mongoDBRunning);
        TestUtil.ignoreException(() -> {
            TestUtil.testCall(db, "CALL apoc.mongodb.get('mongodb://192.168.99.100:27017','test','test',null,true)", r -> {
                Map doc = (Map) r.get("value");
                assertNotNull(doc.get("_id"));
                assertEquals("testDocument", doc.get("name"));
                assertEquals(currentTime.getTime(), doc.get("date"));
            });
        }, MongoTimeoutException.class);
    }

    @Test
    public void testFirst() throws Exception {
        Assume.assumeTrue(mongoDBRunning);
        TestUtil.ignoreException(() -> {
            TestUtil.testCall(db, "CALL apoc.mongodb.first('mongodb://192.168.99.100:27017','test','test',{name:'testDocument'})", r -> {
                Map doc = (Map) r.get("value");
                assertNotNull(doc.get("_id"));
                assertEquals("testDocument", doc.get("name"));
            });
        }, MongoTimeoutException.class);
    }

    @Test
    public void testFind() throws Exception {
        Assume.assumeTrue(mongoDBRunning);
        TestUtil.ignoreException(() -> {
            TestUtil.testCall(db, "CALL apoc.mongodb.find('mongodb://192.168.99.100:27017','test','test',{name:'testDocument'},null,null)", r -> {
                Map doc = (Map) r.get("value");
                assertNotNull(doc.get("_id"));
                assertEquals("testDocument", doc.get("name"));
            });
        }, MongoTimeoutException.class);
    }

    @Test
    public void testFindSort() throws Exception {
        Assume.assumeTrue(mongoDBRunning);
        TestUtil.ignoreException(() -> {
            TestUtil.testCall(db, "CALL apoc.mongodb.find('mongodb://192.168.99.100:27017','test','test',{name:'testDocument'},null,{name:1})", r -> {
                Map doc = (Map) r.get("value");
                assertNotNull(doc.get("_id"));
                assertEquals("testDocument", doc.get("name"));
            });
        }, MongoTimeoutException.class);
    }

    @Test
    public void testCount() throws Exception {
        Assume.assumeTrue(mongoDBRunning);
        TestUtil.ignoreException(() -> {
            TestUtil.testCall(db, "CALL apoc.mongodb.count('mongodb://192.168.99.100:27017','test','test',{name:'testDocument'})", r -> {
                assertEquals(1L, r.get("value"));
            });
        }, MongoTimeoutException.class);
    }

    @Test
    public void testCountAll() throws Exception {
        Assume.assumeTrue(mongoDBRunning);
        TestUtil.ignoreException(() -> {
            TestUtil.testCall(db, "CALL apoc.mongodb.count('mongodb://192.168.99.100:27017','test','test',null)", r -> {
                assertEquals(1L, r.get("value"));
            });
        }, MongoTimeoutException.class);
    }

    @Test
    public void testUpdate() throws Exception {
        Assume.assumeTrue(mongoDBRunning);
        TestUtil.ignoreException(() -> {
            TestUtil.testCall(db, "CALL apoc.mongodb.update('mongodb://192.168.99.100:27017','test','test',{name:'testDocument'},{`$set`:{age:42}})", r -> {
                assertEquals(1L, r.get("value"));
                assertEquals(1L, collection.count(new Document(map("age", 42L))));
            });
        }, MongoTimeoutException.class);
    }

    @Test
    public void testInsert() throws Exception {
        Assume.assumeTrue(mongoDBRunning);
        TestUtil.ignoreException(() -> {
            TestUtil.testCall(db, "CALL apoc.mongodb.insert('mongodb://192.168.99.100:27017','test','test',[{Jon:'Snow'}])", r -> {
                assertEquals(1L, r.get("value"));
                assertEquals(1L, collection.count(new Document(map("Jon", "Snow"))));
            });
        }, MongoTimeoutException.class);
    }

    @Test
    public void testDelete() throws Exception {
        Assume.assumeTrue(mongoDBRunning);
        collection.insertOne(new Document(map("foo", "bar")));
        TestUtil.ignoreException(() -> {
            TestUtil.testCall(db, "CALL apoc.mongodb.delete('mongodb://192.168.99.100:27017','test','test',{foo:'bar'})", r -> {
                assertEquals(1L, r.get("value"));
                assertEquals(0L, collection.count(new Document(map("foo", "bar"))));
            });
        }, MongoTimeoutException.class);
    }

}
