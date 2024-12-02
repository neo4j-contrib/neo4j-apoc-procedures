package apoc.full.it.monitor;

import apoc.util.Neo4jContainerExtension;
import apoc.util.TestContainerUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.neo4j.driver.Session;

import java.util.List;

import static apoc.util.TestContainerUtil.createEnterpriseDB;
import static apoc.util.TestContainerUtil.testCall;
import static org.junit.Assert.assertNotNull;

public class StoreInfoProcedureEnterpriseTest {
    private static Neo4jContainerExtension neo4jContainer;
    private static Session session;

    @BeforeClass
    public static void setUp() throws Exception {
        neo4jContainer = createEnterpriseDB(List.of(TestContainerUtil.ApocPackage.FULL), true);
        neo4jContainer.start();

        session = neo4jContainer.getSession();
    }

    @AfterClass
    public static void tearDown() {
        session.close();
        neo4jContainer.close();
    }

    @Test
    public void testGetStoreInfo() {
        testCall(session, "CALL apoc.monitor.store()", (row) -> {
            assertNotNull(row.get("logSize"));
            assertNotNull(row.get("stringStoreSize"));
            assertNotNull(row.get("arrayStoreSize"));
            assertNotNull(row.get("nodeStoreSize"));
            assertNotNull(row.get("relStoreSize"));
            assertNotNull(row.get("propStoreSize"));
            assertNotNull(row.get("totalStoreSize"));
        });
    }
}
