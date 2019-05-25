package apoc.util;


import org.jetbrains.annotations.NotNull;
import org.neo4j.driver.v1.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Neo4jContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.ext.ScriptUtils;

import java.io.InputStream;
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

    public Neo4jContainerExtension() {}

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
                    tx.success();
                    return null;
                });
            }
        }
    }

    public Session getSession() {
        return session;
    }

    @NotNull
    private AuthToken getAuth() {
        return getAdminPassword() != null && !getAdminPassword().isEmpty()
                ? AuthTokens.basic("neo4j", getAdminPassword()): AuthTokens.none();
    }

    public Neo4jContainerExtension withLogging() {
        withLogConsumer(new Slf4jLogConsumer(logger));
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
