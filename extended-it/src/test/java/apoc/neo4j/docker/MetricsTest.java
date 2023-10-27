package apoc.neo4j.docker;

import apoc.util.Neo4jContainerExtension;
import apoc.util.TestContainerUtil.ApocPackage;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.driver.Record;
import org.neo4j.driver.Session;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static apoc.metrics.Metrics.OUTSIDE_DIR_ERR_MSG;
import static apoc.util.ExtendedFileUtils.NEO4J_DIRECTORY_CONFIGURATION_SETTING_NAMES;
import static apoc.util.TestContainerUtil.*;
import static apoc.util.Util.map;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.neo4j.test.assertion.Assert.assertEventually;

/**
 * @author as
 * @since 13.02.19
 */
// TODO Investigate why this test is not working. Possibly increase timeout for container
@Ignore
public class MetricsTest {

    private static Neo4jContainerExtension neo4jContainer;
    private static Session session;

    @BeforeClass
    public static void beforeAll() throws InterruptedException {
        neo4jContainer = createEnterpriseDB(List.of(ApocPackage.EXTENDED), true)
                .withNeo4jConfig("apoc.import.file.enabled", "true")
                .withNeo4jConfig("metrics.enabled", "true")
                .withNeo4jConfig("metrics.csv.interval", "1s")
                .withNeo4jConfig("metrics.namespaces.enabled", "true");
        neo4jContainer.start();
        session = neo4jContainer.getSession();
    }

    @AfterClass
    public static void afterAll() {
        neo4jContainer.close();
    }
    

    @Test
    public void shouldNotGetFileOutsideMetricsDir() {
        try {
            testCall(session, "CALL apoc.metrics.get('../external')",
                    (r) -> Assert.fail("Should fail because the path is outside the dir "));
        } catch (RuntimeException e) {
            assertEquals("Failed to invoke procedure `apoc.metrics.get`: Caused by: java.lang.RuntimeException: " + OUTSIDE_DIR_ERR_MSG, e.getMessage());
        }
    }
    
    // TODO: Investigate broken test. It hangs for more than 30 seconds for no reason.
    @Test
    @Ignore
    public void shouldGetMetrics() {
        session.readTransaction(tx -> tx.run("RETURN 1 AS num;"));
        String metricKey = "neo4j.system.check_point.total_time";
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
                (r) -> {

            assertTrue("should have at least one element", r.hasNext());
                });
    }

}
