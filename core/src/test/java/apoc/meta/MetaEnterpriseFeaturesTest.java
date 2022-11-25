package apoc.meta;

import apoc.util.Neo4jContainerExtension;
import apoc.util.TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.driver.Session;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import static apoc.util.TestContainerUtil.createEnterpriseDB;
import static apoc.util.TestContainerUtil.testResult;
import static apoc.util.TestUtil.isRunningInCI;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeNotNull;
import static org.junit.Assume.assumeTrue;

/**
 * @author as
 * @since 12.02.19
 */
public class MetaEnterpriseFeaturesTest {

    private static Neo4jContainerExtension neo4jContainer;
    private static Session session;

    @BeforeClass
    public static void beforeAll() {
        assumeFalse(isRunningInCI());
        // We build the project, the artifact will be placed into ./build/libs
        neo4jContainer = createEnterpriseDB(!TestUtil.isRunningInCI());
        neo4jContainer.start();
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

    public static boolean hasRecordMatching(List<Map<String,Object>> records, Predicate<Map<String,Object>> predicate) {
        return records.stream().filter(predicate).count() > 0;
    }

    public static List<Map<String,Object>> gatherRecords(Iterator<Map<String,Object>> r) {
        List<Map<String,Object>> rows = new ArrayList<>();
        while(r.hasNext()) {
            Map<String,Object> row = r.next();
            rows.add(row);
        }
        return rows;
    }

    @Test
    public void testNodeTypePropertiesBasic() {
        session.writeTransaction(tx -> {
            tx.run("CREATE CONSTRAINT ON (f:Foo) ASSERT EXISTS (f.s);");
            tx.commit();
            return null;
        });
        session.writeTransaction(tx -> {
            tx.run("CREATE (:Foo { l: 1, s: 'foo', d: datetime(), ll: ['a', 'b'], dl: [2.0, 3.0] });");
            tx.commit();
            return null;
        });
        testResult(session, "CALL apoc.meta.nodeTypeProperties();", (r) -> {
            List<Map<String,Object>> records = gatherRecords(r);
            assertTrue(hasRecordMatching(records, m ->
                        m.get("nodeType").equals(":`Foo`") &&
                                ((List)m.get("nodeLabels")).get(0).equals("Foo") &&
                                m.get("propertyName").equals("s") &&
                                m.get("mandatory").equals(true)));

            assertTrue(hasRecordMatching(records, m ->
                    m.get("propertyName").equals("s") &&
                            ((List)m.get("propertyTypes")).get(0).equals("String")));

            assertTrue(hasRecordMatching(records, m ->
                    m.get("nodeType").equals(":`Foo`") &&
                            ((List)m.get("nodeLabels")).get(0).equals("Foo") &&
                            m.get("propertyName").equals("dl") &&
                            m.get("mandatory").equals(false)));

            assertEquals(5, records.size());
        });
    }
}
