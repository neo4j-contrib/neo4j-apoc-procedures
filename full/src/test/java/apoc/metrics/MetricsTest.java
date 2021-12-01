package apoc.metrics;

import apoc.util.Neo4jContainerExtension;
import apoc.util.TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.driver.Record;
import org.neo4j.driver.Session;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static apoc.util.FileUtils.NEO4J_DIRECTORY_CONFIGURATION_SETTING_NAMES;
import static apoc.util.TestContainerUtil.createEnterpriseDB;
import static apoc.util.TestContainerUtil.testResult;
import static apoc.util.TestUtil.isRunningInCI;
import static apoc.util.Util.map;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeNotNull;
import static org.junit.Assume.assumeTrue;
import static org.neo4j.test.assertion.Assert.assertEventually;

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
                    .withDebugger()
                .withNeo4jConfig("apoc.import.file.enabled", "true");
            neo4jContainer.start();
        }, Exception.class);
        assumeNotNull(neo4jContainer);
        assumeTrue(neo4jContainer.isRunning());
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
        String metricKey = "neo4j.bolt.connections_opened";
        assertEventually(() -> {
                try {
                    return session.run("CALL apoc.metrics.get($metricKey)",
                                map("metricKey", metricKey))
                        .list()
                        .get(0)
                        .asMap();
                } catch (Exception e) {
                    return Map.<String, Object>of();
                }
            },
            map -> Set.of("timestamp", "metric", "map").equals(map.keySet()) && map.get("metric").equals(metricKey),
    30L, TimeUnit.SECONDS);
    }

    @Test
    public void shouldRetrieveStorageMetrics() {

        final Set<String> expectedSet = Stream.of("setting", "freeSpaceBytes", "totalSpaceBytes", "usableSpaceBytes", "percentFree")
                .collect(Collectors.toSet());

        NEO4J_DIRECTORY_CONFIGURATION_SETTING_NAMES.forEach(setting ->
                assertEventually(() -> {
                            try {
                                return session.run("CALL apoc.metrics.storage($setting)", map("setting", setting))
                                        .single()
                                        .asMap();
                            } catch (Exception e) {
                                return Map.<String, Object>of();
                            }
                        },
                        map -> expectedSet.equals(map.keySet()),
                        30L, TimeUnit.SECONDS)
        );

        // all metrics with null

        assertEventually(() -> {
                    try {
                        return session.run("CALL apoc.metrics.storage(null)")
                                .stream()
                                .map(Record::asMap)
                                .collect(Collectors.toList());
                    } catch (Exception e) {
                        return List.<Map<String, Object>>of();
                    }
                },
                list -> list.stream().allMatch(m -> m.keySet().equals(expectedSet))
                        && list.stream().map(m -> m.get("setting")).collect(Collectors.toSet())
                            .equals(Set.copyOf(NEO4J_DIRECTORY_CONFIGURATION_SETTING_NAMES)),
                30L, TimeUnit.SECONDS);
    }

    @Test
    public void shouldListMetrics() {
        testResult(session, "CALL apoc.metrics.list()",
                (r) -> assertTrue("should have at least one element", r.hasNext()));
    }

}
