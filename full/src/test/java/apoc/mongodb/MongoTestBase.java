package apoc.mongodb;

import apoc.util.JsonUtil;
import apoc.util.TestUtil;
import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.AfterClass;
import org.junit.ClassRule;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.utility.Base58;

import java.time.Duration;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static apoc.util.TestUtil.isRunningInCI;
import static java.net.HttpURLConnection.HTTP_OK;
import static java.net.HttpURLConnection.HTTP_UNAUTHORIZED;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeNotNull;
import static org.junit.Assume.assumeTrue;

public class MongoTestBase {

    public static int MONGO_DEFAULT_PORT = 27017;
    public static final String[] COMMANDS = { "mongo", "admin", "--eval", "db.serverStatus().connections" };

    public static GenericContainer mongo;

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    public static MongoCollection<Document> testCollection;
    public static MongoCollection<Document> productCollection;
    public static MongoCollection<Document> personCollection;

    public static final Date currentTime = new Date();

    public static final long longValue = 10_000L;

    public static String HOST = null;

    public static Map<String, Object> PERSON_PARAMS;
    public static Map<String, Object> TEST_PARAMS;

    public long numConnections = -1;

    public static final long NUM_OF_RECORDS = 10_000L;

    public static List<ObjectId> productReferences;
    public static ObjectId nameAsObjectId = new ObjectId("507f191e810c19729de860ea");
    public static ObjectId fooAlAsObjectId = new ObjectId("77e193d7a9cc81b4027498b4");
    public static ObjectId fooJohnAsObjectId = new ObjectId("57e193d7a9cc81b4027499c4");
    public static ObjectId fooJackAsObjectId = new ObjectId("67e193d7a9cc81b4027518b4");
    public static ObjectId idAlAsObjectId = new ObjectId("97e193d7a9cc81b4027519b4");
    public static ObjectId idJackAsObjectId = new ObjectId("97e193d7a9cc81b4027518b4");
    public static ObjectId idJohnAsObjectId = new ObjectId("07e193d7a9cc81b4027488c4");
    public static ObjectId idAsObjectId = new ObjectId();

    public static final Set<String> SET_OBJECT_ID_MAP = Set.of("date", "machineIdentifier", "processIdentifier", "counter", "time", "timestamp", "timeSecond");


    @AfterClass
    public static void tearDown() {
        if (mongo != null) {
            mongo.stop();
        }
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
//                mongo.withEnv("MONGO_INITDB_ROOT_USERNAME", "user")
//                    .withEnv("MONGO_INITDB_ROOT_PASSWORD", "pass");
                mongo.withEnv(Map.of("MONGO_INITDB_ROOT_USERNAME", "user",
                        "MONGO_INITDB_ROOT_PASSWORD", "pass"
//                        "MONGO_INITDB_DATABASE", "test",
//                        "MONGODB_CLIENT_EXTRA_FLAGS", "--authenticationDatabase=test"
//                        "MONGODB_USERNAME", "user",
//                        "MONGODB_PASSWORD", "pass",
//                        "MONGODB_DATABASE","test"
                ));
            }
            mongo.start();

        }, Exception.class);
        assumeNotNull(mongo);
        assumeTrue("Mongo DB must be running", mongo.isRunning());
    }

    /**
     * Get the number of active connections that can be used to check if them are managed correctly
     * by invoking:
     *  $ mongo test --eval db.serverStatus().connections
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
}
