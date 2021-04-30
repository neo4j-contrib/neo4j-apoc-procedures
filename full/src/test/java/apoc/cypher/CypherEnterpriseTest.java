package apoc.cypher;

import apoc.util.Neo4jContainerExtension;
import apoc.util.TestUtil;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.driver.Session;

import static apoc.util.TestContainerUtil.createEnterpriseDB;
import static apoc.util.TestContainerUtil.testResult;
import static apoc.util.TestUtil.isRunningInCI;
import static apoc.util.Util.map;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeNotNull;
import static org.junit.Assume.assumeTrue;

public class CypherEnterpriseTest {

    private static Neo4jContainerExtension neo4jContainer;
    private static Session session;

    @BeforeClass
    public static void beforeAll() {
        assumeFalse(isRunningInCI());
        TestUtil.ignoreException(() -> {
            // We build the project, the artifact will be placed into ./build/libs
            neo4jContainer = createEnterpriseDB(!TestUtil.isRunningInCI())
                    .withNeo4jConfig("dbms.transaction.timeout", "5s");
            neo4jContainer.start();
        }, Exception.class);
        assumeNotNull(neo4jContainer);
        assumeTrue("Neo4j Instance should be up-and-running", neo4jContainer.isRunning());
        session = neo4jContainer.getSession();
    }

    @AfterClass
    public static void afterAll() {
        if (neo4jContainer != null && neo4jContainer.isRunning()) {
            session.close();
            neo4jContainer.close();
        }
    }

    @Test
    public void testParallelTransactionGuard() {
        // given
        int txCountBefore = neo4jContainer.getSession().readTransaction(tx -> tx.run("CALL dbms.listTransactions()").list()).size();

        // when
        try {
            int size = 10_000;
            testResult(neo4jContainer.getSession(),
                    "CALL apoc.cypher.parallel2('UNWIND range(0,9) as id CALL apoc.util.sleep(10000) WITH id RETURN id', {a: range(1, $size)}, 'a')",
                    map("size", size),
                    r -> {});
        } catch (Exception ignored) {}

        // then
        int txCountAfter = neo4jContainer.getSession().readTransaction(tx -> tx.run("CALL dbms.listTransactions()").list()).size();
        Assert.assertEquals(txCountBefore, txCountAfter);
    }
}
