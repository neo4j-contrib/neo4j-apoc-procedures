package apoc.util;


import org.jetbrains.annotations.NotNull;
import org.neo4j.driver.internal.summary.InternalSummaryCounters;
import org.neo4j.driver.v1.*;
import org.neo4j.driver.v1.summary.SummaryCounters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Neo4jContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.ext.ScriptUtils;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
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
                        x.constraintsRemoved() + y.constraintsRemoved())
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
                "\tconstraintsRemoved: " + sum.constraintsRemoved());
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
