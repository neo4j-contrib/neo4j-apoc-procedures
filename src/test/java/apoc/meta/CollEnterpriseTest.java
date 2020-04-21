package apoc.meta;

import apoc.util.Neo4jContainerExtension;
import apoc.util.TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.driver.Session;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import static apoc.util.TestContainerUtil.createEnterpriseDB;
import static apoc.util.TestContainerUtil.testResult;
import static apoc.util.TestUtil.isTravis;
import static apoc.util.TestUtil.testCall;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeNotNull;

public class CollEnterpriseTest {

    private static Neo4jContainerExtension neo4jContainer;
    private static Session session;

    @BeforeClass
    public static void beforeAll() {
        assumeFalse(isTravis());
        TestUtil.ignoreException(() -> {
            // We build the project, the artifact will be placed into ./build/libs
            neo4jContainer = createEnterpriseDB(!TestUtil.isTravis());
            neo4jContainer.start();
        }, Exception.class);
        assumeNotNull(neo4jContainer);
        session = neo4jContainer.getSession();
    }

    @AfterClass
    public static void afterAll() {
        if (neo4jContainer != null) {
            session.close();
            neo4jContainer.close();
        }
    }

    @Test
    public void testMin() throws Exception {
        assertEquals(1L, session.run("RETURN apoc.coll.min([1,2]) as value").next().get("value").asLong());
        assertEquals(1L, session.run("RETURN apoc.coll.min([1,2,3]) as value").next().get("value").asLong());
        assertEquals(0.5D, session.run("RETURN apoc.coll.min([0.5,1,2.3]) as value").next().get("value").asDouble(), 0.1);
    }

    @Test
    public void testMax() throws Exception {
        assertEquals(3L, session.run("RETURN apoc.coll.max([1,2,3]) as value").next().get("value").asLong());
        assertEquals(2.3D, session.run("RETURN apoc.coll.max([0.5,1,2.3]) as value").next().get("value").asDouble(), 0.1);
    }

}
