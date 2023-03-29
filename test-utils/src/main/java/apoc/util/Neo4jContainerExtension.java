package apoc.util;

import org.neo4j.driver.AuthToken;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.neo4j.driver.internal.summary.InternalSummaryCounters;
import org.neo4j.driver.summary.SummaryCounters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Neo4jContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.containers.wait.strategy.WaitAllStrategy;
import org.testcontainers.containers.wait.strategy.WaitStrategy;
import org.testcontainers.ext.ScriptUtils;

import java.io.InputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import static java.net.HttpURLConnection.HTTP_OK;

/**
 * Extension for the Neo4jcontainer class of Testcontainers
 */
public class Neo4jContainerExtension extends Neo4jContainer<Neo4jContainerExtension> {

    private static final Logger logger = LoggerFactory.getLogger(Neo4jContainerExtension.class);

    private Session session;
    private Driver driver;

    private String filePath;

    private boolean withDriver = true;

    private boolean isRunning = false;

    public Neo4jContainerExtension() {
        super();
    }

    public Neo4jContainerExtension(String dockerImage) {
        // http on 4.0 seems to deliver a 404 first
        setDockerImageName(dockerImage);

        WaitStrategy waitForBolt = new LogMessageWaitStrategy()
                .withRegEx(String.format(".*Bolt enabled on (0\\.0\\.0\\.0:%d|\\[0:0:0:0:0:0:0:0%%0\\]:%1$s)\\.\n", 7687));
        WaitStrategy waitForHttp = new HttpWaitStrategy()
                .forPort(7474)
                .forStatusCodeMatching(response -> response == HTTP_OK);

        setWaitStrategy(new WaitAllStrategy()
                .withStrategy(waitForBolt)
                .withStrategy(waitForHttp)
                .withStartupTimeout(Duration.ofMinutes(5)));
    }

    public Neo4jContainerExtension withInitScript(String filePath) {
        this.filePath = filePath;
        return this;
    }

    public Neo4jContainerExtension withoutDriver() {
        this.withDriver = false;
        return this;
    }

    @Override
    public void start() {
        super.start();
        if (withDriver) {
            driver = GraphDatabase.driver(getBoltUrl(), getAuth());
            session = driver.session();
            if (filePath != null && !filePath.isEmpty()) {
                executeScript(filePath);
            }
        }
        isRunning = true;
    }

    private void executeScript(String filePath) {
        InputStream resource = Thread.currentThread().getContextClassLoader().getResourceAsStream(filePath);
        if (resource == null) {
            logger().warn("Could not load classpath init script: {}", filePath);
            throw new ScriptUtils.ScriptLoadException("Could not load classpath init script: " + filePath + ". Resource not found.");
        }

        List<SummaryCounters> counters = new ArrayList<>();
        try (Scanner scanner = new Scanner(resource).useDelimiter(";")) {
            while (scanner.hasNext()) {
                String statement = scanner.next().trim();
                if (statement.isEmpty()) {
                    continue;
                }
                counters.add(session.writeTransaction(tx -> tx.run(statement).consume().counters()));
            }
        }
        if (counters.isEmpty()) return;

        SummaryCounters sum = counters.stream().reduce(InternalSummaryCounters.EMPTY_STATS, (x, y) ->
                new InternalSummaryCounters(x.nodesCreated() + y.nodesCreated(),
                        x.nodesDeleted() + y.nodesDeleted(),
                        x.relationshipsCreated() + y.relationshipsCreated(),
                        x.relationshipsDeleted() + y.relationshipsDeleted(),
                        x.propertiesSet() + y.propertiesSet(),
                        x.labelsAdded() + y.labelsAdded(),
                        x.labelsRemoved() + y.labelsRemoved(),
                        x.indexesAdded() + y.indexesAdded(),
                        x.indexesRemoved() + y.indexesRemoved(),
                        x.constraintsAdded() + y.constraintsAdded(),
                        x.constraintsRemoved() + y.constraintsRemoved(),
                        x.systemUpdates() + y.systemUpdates())
        );
        logger().info("Dataset creation report:\n" +
                "\tnodesCreated: " + sum.nodesCreated() + "\n" +
                "\tnodesDeleted: " + sum.nodesDeleted() + "\n" +
                "\trelationshipsCreated: " + sum.relationshipsCreated() + "\n" +
                "\trelationshipsDeleted: " + sum.relationshipsDeleted() + "\n" +
                "\tpropertiesSet: " + sum.propertiesSet() + "\n" +
                "\tlabelsAdded: " + sum.labelsAdded() + "\n" +
                "\tlabelsRemoved: " + sum.labelsRemoved() + "\n" +
                "\tindexesAdded: " + sum.indexesAdded() + "\n" +
                "\tindexesRemoved: " + sum.indexesRemoved() + "\n" +
                "\tconstraintsAdded: " + sum.constraintsAdded() + "\n" +
                "\tconstraintsRemoved: " + sum.constraintsRemoved() + "\n" +
                "\tsystemUpdates: " + sum.systemUpdates());
    }

    public Driver getDriver() {
        return driver;
    }

    public Session getSession() {
        return session;
    }

    public AuthToken getAuth() {
        return getAdminPassword() != null && !getAdminPassword().isEmpty()
                ? AuthTokens.basic("neo4j", getAdminPassword()): AuthTokens.none();
    }

    public Neo4jContainerExtension withLogging() {
        withLogConsumer(new Slf4jLogConsumer(logger));
        return this;
    }

    public Neo4jContainerExtension withDebugger() {
        withEnv("NEO4J_dbms_jvm_additional","-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=0.0.0.0:5005");
        addFixedExposedPort(5005, 5005);
        withExposedPorts(5005);
        return this;
    }

    @Override
    public void stop() {
        if (withDriver) {
            closeSafely(session);
            closeSafely(driver);
        }
        super.stop();
    }

    private static void closeSafely(AutoCloseable closeable) {
        try {
            if (closeable != null) closeable.close();
        } catch (Exception ignoed) {}
    }

    @Override
    public boolean isRunning() {
        return super.isRunning() && isRunning;
    }
}
