package apoc.mongodb;

import apoc.es.ElasticSearch;
import apoc.util.TestUtil;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientURI;
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

import java.net.ConnectException;
import java.util.Collections;
import java.util.Map;

import static apoc.util.MapUtil.map;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
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

    @BeforeClass
    public static void setUp() throws Exception {
        TestUtil.ignoreException(() -> {
            MongoClientOptions options = MongoClientOptions.builder()
                    .socketTimeout(100).serverSelectionTimeout(100)
                    .heartbeatConnectTimeout(100).heartbeatSocketTimeout(100).heartbeatFrequency(100).minHeartbeatFrequency(100)
                    .maxWaitTime(100).connectTimeout(100).build();
            mongoClient = new MongoClient("localhost", options);
            MongoDatabase database = mongoClient.getDatabase("test");
            collection = database.getCollection("test");
            collection.deleteMany(new Document());
            collection.insertOne(new Document(map("name", "testDocument")));
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
        if (db!=null) db.shutdown();
    }

    @Test
    public void testGet() throws Exception {
        Assume.assumeTrue(mongoDBRunning);
        TestUtil.ignoreException(() -> {
            TestUtil.testCall(db, "CALL apoc.mongodb.get('mongodb://localhost:27017','test','test',null)", r -> {
                Map doc = (Map) r.get("value");
                System.out.println("doc = " + doc);
                assertNotNull(doc.get("_id"));
                assertEquals("testDocument", doc.get("name"));
            });
        }, MongoTimeoutException.class);
    }
    @Test
    public void testFirst() throws Exception {
        Assume.assumeTrue(mongoDBRunning);
        TestUtil.ignoreException(() -> {
            TestUtil.testCall(db, "CALL apoc.mongodb.first('mongodb://localhost:27017','test','test',{name:'testDocument'})", r -> {
                Map doc = (Map) r.get("value");
                System.out.println("doc = " + doc);
                assertNotNull(doc.get("_id"));
                assertEquals("testDocument", doc.get("name"));
            });
        }, MongoTimeoutException.class);
    }
    @Test
    public void testFind() throws Exception {
        Assume.assumeTrue(mongoDBRunning);
        TestUtil.ignoreException(() -> {
            TestUtil.testCall(db, "CALL apoc.mongodb.find('mongodb://localhost:27017','test','test',{name:'testDocument'},null,null)", r -> {
                Map doc = (Map) r.get("value");
                System.out.println("doc = " + doc);
                assertNotNull(doc.get("_id"));
                assertEquals("testDocument", doc.get("name"));
            });
        }, MongoTimeoutException.class);
    }
    @Test
    public void testFindSort() throws Exception {
        Assume.assumeTrue(mongoDBRunning);
        TestUtil.ignoreException(() -> {
            TestUtil.testCall(db, "CALL apoc.mongodb.find('mongodb://localhost:27017','test','test',{name:'testDocument'},null,{name:1})", r -> {
                Map doc = (Map) r.get("value");
                System.out.println("doc = " + doc);
                assertNotNull(doc.get("_id"));
                assertEquals("testDocument", doc.get("name"));
            });
        }, MongoTimeoutException.class);
    }
    @Test
    public void testCount() throws Exception {
        Assume.assumeTrue(mongoDBRunning);
        TestUtil.ignoreException(() -> {
            TestUtil.testCall(db, "CALL apoc.mongodb.count('mongodb://localhost:27017','test','test',{name:'testDocument'})", r -> {
                assertEquals(1L, r.get("value"));
            });
        }, MongoTimeoutException.class);
    }

    @Test
    public void testCountAll() throws Exception {
        Assume.assumeTrue(mongoDBRunning);
        TestUtil.ignoreException(() -> {
            TestUtil.testCall(db, "CALL apoc.mongodb.count('mongodb://localhost:27017','test','test',null)", r -> {
                assertEquals(1L, r.get("value"));
            });
        }, MongoTimeoutException.class);
    }

    @Test
    public void testUpdate() throws Exception {
        Assume.assumeTrue(mongoDBRunning);
        TestUtil.ignoreException(() -> {
            TestUtil.testCall(db, "CALL apoc.mongodb.update('mongodb://localhost:27017','test','test',{name:'testDocument'},{`$set`:{age:42}})", r -> {
                assertEquals(1L, r.get("value"));
                assertEquals(1L, collection.count(new Document(map("age",42L))));
            });
        }, MongoTimeoutException.class);
    }
    @Test
    public void testInsert() throws Exception {
        Assume.assumeTrue(mongoDBRunning);
        TestUtil.ignoreException(() -> {
            TestUtil.testCall(db, "CALL apoc.mongodb.insert('mongodb://localhost:27017','test','test',[{Jon:'Snow'}])", r -> {
                assertEquals(1L, r.get("value"));
                assertEquals(1L, collection.count(new Document(map("Jon","Snow"))));
            });
        }, MongoTimeoutException.class);
    }
    @Test
    public void testDelete() throws Exception {
        Assume.assumeTrue(mongoDBRunning);
        collection.insertOne(new Document(map("foo","bar")));
        TestUtil.ignoreException(() -> {
            TestUtil.testCall(db, "CALL apoc.mongodb.delete('mongodb://localhost:27017','test','test',{foo:'bar'})", r -> {
                assertEquals(1L, r.get("value"));
                assertEquals(0L, collection.count(new Document(map("foo","bar"))));
            });
        }, MongoTimeoutException.class);
    }

}
