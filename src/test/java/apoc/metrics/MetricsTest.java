package apoc.metrics;

import apoc.util.Neo4jContainerExtension;
import apoc.util.TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.driver.v1.Session;

import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static apoc.util.TestContainerUtil.*;
import static apoc.util.TestUtil.isTravis;
import static apoc.util.Util.map;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeNotNull;

/**
 * @author as
 * @since 13.02.19
 */
public class MetricsTest {

    private static Neo4jContainerExtension neo4jContainer;
    private static Session session;

    @BeforeClass
    public static void beforeAll() {
        assumeFalse(isTravis());
        TestUtil.ignoreException(() -> {
            executeGradleTasks("clean", "shadow");
            neo4jContainer = createEnterpriseDB(true)
                .withNeo4jConfig("apoc.import.file.enabled", "true");
            neo4jContainer.start();
        }, Exception.class);
        assumeNotNull(neo4jContainer);
        session = neo4jContainer.getSession();
    }

    @AfterClass
    public static void afterAll() {
        if (neo4jContainer != null) {
            neo4jContainer.close();
        }
        cleanBuild();
    }

    @Test
    public void shouldGetMetrics() {
        session.readTransaction(tx -> tx.run("RETURN 1 AS num;"));
        String metricKey = "neo4j.bolt.sessions_started";
        testResult(session, "CALL apoc.metrics.get({metricKey})",
                map("metricKey", metricKey), (r) -> {
                    assertTrue("should have at least one element", r.hasNext());
                    Map<String, Object> map = r.next();
                    assertEquals(Stream.of("timestamp", "metric", "map").collect(Collectors.toSet()), map.keySet());
                    assertEquals(metricKey, map.get("metric"));
                });
    }

    @Test
    public void shouldListMetrics() {
        testResult(session, "CALL apoc.metrics.list()",
                Collections.emptyMap(),
                (r) -> assertTrue("should have at least one element", r.hasNext()));
    }

}