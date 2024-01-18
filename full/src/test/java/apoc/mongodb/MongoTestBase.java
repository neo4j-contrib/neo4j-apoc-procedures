package apoc.mongodb;

import apoc.util.JsonUtil;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.ClassRule;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Result;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.utility.Base58;

import java.text.ParseException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static apoc.util.MapUtil.map;
import static java.net.HttpURLConnection.HTTP_OK;
import static java.net.HttpURLConnection.HTTP_UNAUTHORIZED;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class MongoTestBase {
    enum MongoVersion {
        FOUR("mongo:4", "mongo"),
        LATEST("mongo:7.0.4", "mongosh");

        public final String dockerImg;
        public final String shell;

        MongoVersion(String dockerImg, String shell) {
            this.dockerImg = dockerImg;
            this.shell = shell;
        }
    }
    
    private long numConnections = -1;

    protected static final int MONGO_DEFAULT_PORT = 27017;
    protected static final long NUM_OF_RECORDS = 10_000L;
    protected static final Set<String> SET_OBJECT_ID_MAP = Set.of("date", "timestamp");

    protected static String[] commands;

    protected static GenericContainer mongo;

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    protected static MongoCollection<Document> testCollection;
    protected static MongoCollection<Document> productCollection;
    protected static MongoCollection<Document> personCollection;

    protected static final Date currentTime = new Date();
    protected static final long longValue = 10_000L;

    protected static List<ObjectId> productReferences;
    protected static ObjectId nameAsObjectId = new ObjectId("507f191e810c19729de860ea");
    protected static ObjectId idAsObjectId = new ObjectId();
    protected static List<String> boughtListObjectIds;

    @AfterClass
    public static void tearDown() {
        mongo.stop();
        db.shutdown();
    }

    @Before
    public void before() {
        numConnections = (long) getNumConnections(mongo, commands).get("current");
    }

    @After
    public void after() {
        // the connections active before must be equal to the connections active after
        long numConnectionsAfter = (long) getNumConnections(mongo, commands).get("current");
        assertEquals(numConnections, numConnectionsAfter);
    }

    public static void createContainer(boolean withAuth, MongoVersion mongoVersion) {
        mongo = new GenericContainer(mongoVersion.dockerImg)
                .withNetworkAliases("mongo-" + Base58.randomString(6))
                .withExposedPorts(MONGO_DEFAULT_PORT)
                .waitingFor(new HttpWaitStrategy()
                        .forPort(MONGO_DEFAULT_PORT)
                        .forStatusCodeMatching(response -> response == HTTP_OK || response == HTTP_UNAUTHORIZED)
                        .withStartupTimeout(Duration.ofMinutes(2)));

        if (withAuth) {
            mongo.withEnv("MONGO_INITDB_ROOT_USERNAME", "admin")
                .withEnv("MONGO_INITDB_ROOT_PASSWORD", "pass");

            commands = new String[]{mongoVersion.shell, "admin", "--eval", "db.auth('admin', 'pass'); db.serverStatus().connections;"};
        } else {
            commands = new String[]{mongoVersion.shell, "test", "--eval", "db.serverStatus().connections"};
        }
        mongo.start();
    }

    protected static void fillDb(MongoClient mongoClient) throws ParseException {
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
        boughtListObjectIds = productReferences.stream().map(ObjectId::toString).collect(Collectors.toList());

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
    }

    /**
     * Get the number of active connections that can be used to check if them are managed correctly
     * by invoking:
     *  $ mongo [dbName] --eval db.serverStatus().connections
     * into the container
     * @return A Map<String, Long> with three fields {current, available, totalCreated}
     */
    public static Map<String, Object> getNumConnections(GenericContainer mongo, String ...commands) {
        try {
            Container.ExecResult execResult = mongo.execInContainer(commands);
            assertTrue("stderr is empty", execResult.getStderr() == null || execResult.getStderr().isEmpty());
            assertTrue("stdout is not empty", execResult.getStdout() != null && !execResult.getStdout().isEmpty());

            List<String> lists = Stream.of(execResult.getStdout().split("\n"))
                    .filter(StringUtils::isNotBlank)
                    .collect(Collectors.toList());
            lists = lists.subList(lists.indexOf("{"), lists.size());
            String jsonStr = String.join("", lists);
            return JsonUtil.OBJECT_MAPPER.readValue(jsonStr, Map.class);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected static void assertResult(Result res) {
        assertResult(res, currentTime.toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime());
    }

    protected static void assertResult(Result res, Object date) {
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

    protected static void assertBoughtReferences(Map<String, Object> value, boolean idAsMap, boolean compatibleValues) {
        List<Object> boughtList = (List<Object>) value.get("bought");
        Map<String, Object> firstBought = (Map<String, Object>) boughtList.get(0);
        Map<String, Object> secondBought = (Map<String, Object>) boughtList.get(1);
        final Object firstId = firstBought.get("_id");
        final Object secondId = secondBought.get("_id");
        if (idAsMap) {
            assertEquals(SET_OBJECT_ID_MAP, ((Map<String, Object>) firstId).keySet());
            assertEquals(SET_OBJECT_ID_MAP, ((Map<String, Object>) secondId).keySet());
        } else {
            assertEquals(boughtListObjectIds.get(0), firstId);
            assertEquals(boughtListObjectIds.get(1), secondId);
        }
        final Object firstPrice = firstBought.get("price");
        final Object secondPrice = secondBought.get("price");
        if (compatibleValues) {
            assertEquals(800L, firstPrice);
            assertEquals(1200L, secondPrice);
        } else {
            assertEquals(800, firstPrice);
            assertEquals(1200, secondPrice);
        }
        assertEquals(Arrays.asList("Tech", "Mobile", "Phone", "iOS"), firstBought.get("tags"));
        assertEquals(Arrays.asList("Tech", "Mobile", "Phone", "Android"), secondBought.get("tags"));
    }

    protected static void assertionsInsertDataWithFromDocument(Date date, Result r) {
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
        Map<String, Object> product1Map = map("name", "My Awesome Product", "price", 800L);
        assertEquals(product1Map, product1.getProperties("name", "price"));
        assertArrayEquals(new String[]{"Tech", "Mobile", "Phone", "iOS"}, (String[]) product1.getProperty("tags"));
        assertEquals(Arrays.asList(Label.label("Product")), product1.getLabels());

        Node product2 = products.get(1);
        Map<String, Object> product2Map = map("name", "My Awesome Product 2", "price", 1200L);
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
    }
}
