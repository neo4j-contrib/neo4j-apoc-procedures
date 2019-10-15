package apoc.util;

import org.neo4j.driver.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Neo4jContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.containers.wait.strategy.WaitAllStrategy;
import org.testcontainers.containers.wait.strategy.WaitStrategy;
import org.testcontainers.ext.ScriptUtils;

import java.io.InputStream;
import java.time.Duration;
import java.util.Scanner;

/**
 * Extension for the Neo4jcontainer class of Testcontainers
 */
public class Neo4jContainerExtension extends Neo4jContainer<Neo4jContainerExtension> {

    private static final Logger logger = LoggerFactory.getLogger(Neo4jContainerExtension.class);

    private Session session;
    private Driver driver;

    private String filePath;

    private boolean withDriver = true;

    public Neo4jContainerExtension() {
        super();
    }

    public Neo4jContainerExtension(String dockerImage) {
        // http on 4.0 seems to deliver a 404 first
        setDockerImageName(dockerImage);

        WaitStrategy waitForBolt = new LogMessageWaitStrategy()
                .withRegEx(String.format(".*Bolt enabled on 0\\.0\\.0\\.0:%d\\.\n", 7687));

        this.waitStrategy = new WaitAllStrategy()
                .withStrategy(waitForBolt)
                .withStartupTimeout(Duration.ofMinutes(2));
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
    }

    private void executeScript(String filePath) {
        InputStream resource = Thread.currentThread().getContextClassLoader().getResourceAsStream(filePath);
        if (resource == null) {
            logger().warn("Could not load classpath init script: {}", filePath);
            throw new ScriptUtils.ScriptLoadException("Could not load classpath init script: " + filePath + ". Resource not found.");
        }

        try (Scanner scanner = new Scanner(resource).useDelimiter(";")) {
            while (scanner.hasNext()) {
                String statement = scanner.next().trim();
                if (statement.isEmpty()) {
                    continue;
                }
                session.writeTransaction(tx -> {
                    tx.run(statement);
                    tx.commit();
                    return null;
                });
            }
        }
    }

    public Session getSession() {
        return session;
    }

    private AuthToken getAuth() {
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
            session.close();
            driver.close();
        }
        super.stop();
    }
}
