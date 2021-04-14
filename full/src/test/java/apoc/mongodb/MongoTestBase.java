package apoc.mongodb;

import apoc.util.JsonUtil;
import apoc.util.TestUtil;
import com.mongodb.client.MongoCollection;
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

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static apoc.util.TestUtil.isRunningInCI;
import static java.net.HttpURLConnection.HTTP_OK;
import static java.net.HttpURLConnection.HTTP_UNAUTHORIZED;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeNotNull;
import static org.junit.Assume.assumeTrue;

public class MongoTestBase {

    public static int MONGO_DEFAULT_PORT = 27017;
    public static String[] COMMANDS;

    public static GenericContainer mongo;

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    public static MongoCollection<Document> testCollection;
    public static MongoCollection<Document> productCollection;
    public static MongoCollection<Document> personCollection;

    public static final Date currentTime = new Date();

    public static final long longValue = 10_000L;

    public static String HOST = null;

    public long numConnections = -1;

    public static final long NUM_OF_RECORDS = 10_000L;

    public static List<ObjectId> productReferences;
    public static ObjectId nameAsObjectId = new ObjectId("507f191e810c19729de860ea");
    public static ObjectId idAsObjectId = new ObjectId();

    @AfterClass
    public static void tearDown() {
        if (mongo != null) {
            mongo.stop();
        }
    }

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

    public static void createContainer(boolean withAuth) {
        assumeFalse(isRunningInCI());
        TestUtil.ignoreException(() -> {
            mongo = new GenericContainer("mongo:3")
                    .withNetworkAliases("mongo-" + Base58.randomString(6))
                    .withExposedPorts(MONGO_DEFAULT_PORT)
                    .waitingFor(new HttpWaitStrategy()
                            .forPort(MONGO_DEFAULT_PORT)
                            .forStatusCodeMatching(response -> response == HTTP_OK || response == HTTP_UNAUTHORIZED)
                            .withStartupTimeout(Duration.ofMinutes(2)));

            if (withAuth) {
                mongo.withEnv("MONGO_INITDB_ROOT_USERNAME", "admin")
                    .withEnv("MONGO_INITDB_ROOT_PASSWORD", "pass");

                COMMANDS = new String[]{"mongo", "admin", "--eval", "db.auth('admin', 'pass'); db.serverStatus().connections;"};
            } else {
                COMMANDS = new String[]{"mongo", "test", "--eval", "db.serverStatus().connections"};
            }
            mongo.start();

        }, Exception.class);
        assumeNotNull(mongo);
        assumeTrue("Mongo DB must be running", mongo.isRunning());
    }

    /**
     * Get the number of active connections that can be used to check if them are managed correctly
     * by invoking:
     *  $ mongo [dbName] --eval db.serverStatus().connections
     * into the container
     * @return A Map<String, Long> with three fields three fields {current, available, totalCreated}
     * @throws Exception
     */
    public static Map<String, Object> getNumConnections(GenericContainer mongo, String ...commands) {
        try {
            Container.ExecResult execResult = mongo.execInContainer(commands);
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

    public static void assertionsInsertDataWithFromDocument(Date date, Result r) {
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
    }
}
