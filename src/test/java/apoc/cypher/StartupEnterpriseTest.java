package apoc.cypher;

import apoc.util.Neo4jContainerExtension;
import apoc.util.TestUtil;
import org.junit.Test;

import static apoc.util.TestContainerUtil.createEnterpriseDB;
import static org.junit.Assert.fail;

public class StartupEnterpriseTest {
    private static Neo4jContainerExtension neo4jContainer;

    @Test
    public void testStartups() {
        neo4jContainer = createEnterpriseDB(!TestUtil.isTravis()).withNeo4jConfig("dbms.transaction.timeout", "5s");

        try {
            neo4jContainer.start();
        } catch (Exception ex) {
            fail("Should not have thrown exception when trying to start Neo4j: " + ex);
        }

        if (neo4jContainer != null) {
            neo4jContainer.close();
        }
    }
}
