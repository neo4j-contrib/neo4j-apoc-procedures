package apoc.metrics;

import apoc.util.Neo4jContainerExtension;
import apoc.util.TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.driver.Session;
import org.neo4j.internal.helpers.collection.Iterators;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static apoc.util.FileUtils.NEO4J_DIRECTORY_CONFIGURATION_SETTING_NAMES;
import static apoc.util.TestContainerUtil.createEnterpriseDB;
import static apoc.util.TestContainerUtil.testCall;
import static apoc.util.TestContainerUtil.testResult;
import static apoc.util.TestUtil.isRunningInCI;
import static apoc.util.Util.map;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeNotNull;
import static org.junit.Assume.assumeTrue;

/**
 * @author as
 * @since 13.02.19
 */
public class MetricsTest {

    private static Neo4jContainerExtension neo4jContainer;
    private static Session session;

    @BeforeClass
    public static void beforeAll() throws InterruptedException {
        assumeFalse(isRunningInCI());
        TestUtil.ignoreException(() -> {
            neo4jContainer = createEnterpriseDB(true)
                    .withNeo4jConfig("apoc.import.file.enabled", "true")
                    .withNeo4jConfig("metrics.enabled", "true")
                    .withNeo4jConfig("metrics.csv.interval", "1s")
                    .withNeo4jConfig("metrics.namespaces.enabled", "true");
            neo4jContainer.start();
        }, Exception.class);
        assumeNotNull(neo4jContainer);
        assumeTrue("Neo4j Instance should be up-and-running", neo4jContainer.isRunning());
        session = neo4jContainer.getSession();

    }

    @AfterClass
    public static void afterAll() {
        if (neo4jContainer != null && neo4jContainer.isRunning()) {
            neo4jContainer.close();
        }
    }

    @Test
    public void shouldGetMetrics() {
        session.readTransaction(tx -> tx.run("RETURN 1 AS num;"));
        String metricKey = "neo4j.database.system.check_point.total_time";
        testResult(session, "CALL apoc.metrics.get($metricKey)",
                map("metricKey", metricKey), (r) -> {
                    assertTrue("should have at least one element", r.hasNext());
                    Map<String, Object> map = r.next();
                    assertEquals(Stream.of("timestamp", "metric", "map").collect(Collectors.toSet()), map.keySet());
                    assertEquals(metricKey, map.get("metric"));
                });
    }

    @Test
    public void shouldRetrieveStorageMetrics() {

        final Set<String> expectedSet = Stream.of("setting", "freeSpaceBytes", "totalSpaceBytes", "usableSpaceBytes", "percentFree")
                .collect(Collectors.toSet());

        NEO4J_DIRECTORY_CONFIGURATION_SETTING_NAMES.forEach(setting ->
                testCall(session, "CALL apoc.metrics.storage($setting)", map("setting", setting), (res) -> {
                    assertEquals(expectedSet, res.keySet());
                    assertEquals(setting, res.get("setting"));
                })
        );

        // all metrics with null
        testResult(session, "CALL apoc.metrics.storage(null)", (res) -> {
            final List<Map<String, Object>> maps = Iterators.asList(res);
            final Set<String> setSetting = maps.stream().map(item -> {
                assertEquals(expectedSet, item.keySet());

                return (String) item.get("setting");
            }).collect(Collectors.toSet());

            assertEquals(new HashSet<>(NEO4J_DIRECTORY_CONFIGURATION_SETTING_NAMES), setSetting);
        });
    }

    @Test
    public void shouldListMetrics() {
        testResult(session, "CALL apoc.metrics.list()",
                (r) -> assertTrue("should have at least one element", r.hasNext()));
    }

}
