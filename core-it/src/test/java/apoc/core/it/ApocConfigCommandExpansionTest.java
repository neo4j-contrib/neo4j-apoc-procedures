package apoc.core.it;

import apoc.util.Neo4jContainerExtension;
import apoc.util.TestContainerUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.driver.Session;

import java.io.File;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;

import static apoc.util.TestContainerUtil.createEnterpriseDB;
import static apoc.util.TestContainerUtil.testResult;
import static org.junit.Assert.assertEquals;

public class ApocConfigCommandExpansionTest {

    private static Neo4jContainerExtension neo4jContainer;
    private static Session session;

    @Before
    public void before() throws URISyntaxException {
        File apocConfigCommandExpansionFile = new File(getClass().getClassLoader().getResource("apoc-config-command-expansion/apoc.conf").toURI());
        neo4jContainer = createEnterpriseDB(
                List.of(TestContainerUtil.ApocPackage.CORE),
                true,
                false,
                true,
                apocConfigCommandExpansionFile
        );
        neo4jContainer.start();
        session = neo4jContainer.getSession();
    }

    @After
    public void after() {
        session.close();
        neo4jContainer.close();
    }

    @Test
    public void testApocConfigWithCommandExpansion() {
        testResult(session,
                "CALL apoc.config.list() " +
                        "YIELD key, value " +
                        "WITH key, value " +
                        "WHERE key = \"apoc.spatial.geocode.osm.throttle\" " +
                        "RETURN value", r -> {
                    Map<String, Object> row = r.next();
                    assertEquals(500, row.get("value"));
                });
    }
}
