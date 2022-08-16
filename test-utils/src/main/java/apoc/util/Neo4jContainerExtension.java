package apoc.util;

import org.apache.commons.io.FileUtils;
import org.neo4j.driver.AuthToken;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Neo4jContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.containers.wait.strategy.WaitAllStrategy;
import org.testcontainers.containers.wait.strategy.WaitStrategy;
import org.testcontainers.ext.ScriptUtils;
import org.testcontainers.containers.wait.strategy.Wait;
import static apoc.util.TestContainerUtil.Neo4jVersion;
import static apoc.util.TestContainerUtil.Neo4jVersion.ENTERPRISE;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
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
        setDockerImageName(dockerImage);
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
        try {
            super.start();
            if (withDriver) {
                driver = GraphDatabase.driver(getBoltUrl(), getAuth());
                session = driver.session();
                if (filePath != null && !filePath.isEmpty()) {
                    executeScript(filePath);
                }
            }
            isRunning = true;
        } catch (Exception startException) {
            try {
                System.out.println(this.execInContainer("cat", "logs/debug.log").toString());
                System.out.println(this.execInContainer("cat", "logs/http.log").toString());
                System.out.println(this.execInContainer("cat", "logs/security.log").toString());
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
            throw startException;
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

    public Driver getDriver() {
        return driver;
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

    private Neo4jContainerExtension withWaitForDatabaseReady(
            String username, String password, String database, Duration timeout, TestContainerUtil.Neo4jVersion version) {
        if (version == ENTERPRISE) {
            this.setWaitStrategy(Wait.forHttp("/db/" + database + "/cluster/available")
                    .withBasicCredentials(username, password)
                    .forPort(7474)
                    .forStatusCodeMatching(t -> {
                        logger.debug("/db/" + database + "/cluster/available [" + t.toString() + "]");
                        return t == 200;
                    })
                    .withReadTimeout(Duration.ofSeconds(3))
                    .withStartupTimeout(timeout));
        } else {
            this.setWaitStrategy(Wait.forHttp("/")
                    .forPort(7474)
                    .forStatusCodeMatching(t -> {
                        logger.debug("/ [" + t.toString() + "]");
                        return t == 200;
                    })
                    .withReadTimeout(Duration.ofSeconds(3))
                    .withStartupTimeout(timeout));
        }

        return this;
    }

    public Neo4jContainerExtension withWaitForNeo4jDatabaseReady(String password, Neo4jVersion version) {
        return withWaitForDatabaseReady("neo4j", password, "neo4j", Duration.ofSeconds(120), version);
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
