package apoc.index;

import apoc.ApocKernelExtensionFactory;
import apoc.util.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.Arrays;
import java.util.Collection;

import static apoc.util.TestUtil.*;
import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class IndexUpdateTransactionEventHandlerTest {

    private GraphDatabaseService db;
    private IndexUpdateTransactionEventHandler indexUpdateTransactionEventHandler;

    @Parameterized.Parameters(name = "async index updates {0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                { false },
                { true}
        });
    }

    @Parameterized.Parameter(value = 0)
    public boolean asyncIndexUpdates;


    @Before
    public void setUp() throws Exception {
        db = new TestGraphDatabaseFactory()
                .newImpermanentDatabaseBuilder()
            .setConfig("apoc.autoIndex.enabled", "true")
            .setConfig("apoc.autoIndex.async", Boolean.toString(asyncIndexUpdates))
            .setConfig("apoc.autoIndex.async_rollover_millis", "200")
            .newGraphDatabase();
        TestUtil.registerProcedure(db, FreeTextSearch.class);
        TestUtil.registerProcedure(db, FulltextIndex.class);

        final ApocKernelExtensionFactory.ApocLifecycle apocLifecycle = ((GraphDatabaseAPI) db).getDependencyResolver().resolveDependency(ApocKernelExtensionFactory.ApocLifecycle.class);
        indexUpdateTransactionEventHandler = apocLifecycle.getIndexUpdateLifeCycle().getIndexUpdateTransactionEventHandler();
    }

    @After
    public void tearDown() {
        db.shutdown();
    }

    @Test
    public void shouldDeletingIndexedNodesSucceed() {
        // setup: create index, add a node
        testCallEmpty(db, "call apoc.index.addAllNodesExtended('search_index',{City:['name']},{autoUpdate:true})", null);
        testCallEmpty(db, "create (c:City{name:\"Made Up City\",url:\"/places/nowhere/made-up-city\"})", null);
        indexUpdateTransactionEventHandler.forceTxRollover();

        // check if we find the node
        testCallCount(db, "CALL apoc.index.nodes('search_index','City.name:\"Made Up\"')", null, 1);

        // when
        TestUtil.testCall(db, "match (c:City{name:'Made Up City'}) delete c return count(c) as count", map -> assertEquals(1L, map.get("count")));
        indexUpdateTransactionEventHandler.forceTxRollover();

        // nothing found in the index after deletion
        testCallCount(db, "CALL apoc.index.nodes('search_index','City.name:\"Made Up\"')", null, 0);
    }


    @Test
    public void shouldIndexFieldsBeUsedConsistently() throws InterruptedException {
        // setup: add a node, index it and add another node
        testCallEmpty(db, "create (c:City{name:\"Made Up City\",url:\"/places/nowhere/made-up-city\"})", null);

        testCallCount(db, "call apoc.index.addAllNodesExtended('search_index',{City:['name']},{autoUpdate:true})", null, 1);

        testCallCount(db, "create (c:City{name:\"Made Up City 2\",url:\"/places/nowhere/made-up-city\"}) return c",
                null, 1);

        indexUpdateTransactionEventHandler.forceTxRollover();
        // when & then
        testCallCount(db, "call apoc.index.search('search_index', 'City.name:Made') yield node, weight return node, weight", null, 2);
        testCallCount(db, "CALL apoc.index.nodes('search_index','name:\"Made Up\"')", null, 0);
        testCallCount(db, "CALL apoc.index.nodes('search_index','City.name:\"Made Up\"')", null, 2);
    }

    @Test
    public void shouldRemovingPropertiesWork() {
        //setup
        testCallEmpty(db, "call apoc.index.addAllNodesExtended('submarines',{Submarine:['color']},{autoUpdate:true})", null);
        testCallEmpty(db, "create (s:Submarine{color:\"yellow\",periscope:true})", null);
        testCallEmpty(db, "create (s:Submarine{color:\"green\"})", null);

        // when & then
        testCallCount(db, "match (s:Submarine) remove s.periscope return s", null, 2);
    }

    @Ignore("this test is supposed to fail until 3.4.10 gets released, https://github.com/neo4j/neo4j/commit/7b8baa607cd63a70303437de7ebb1e254a9e42ff")
    @Test
    public void shouldDeletingNodeWork() {
        testCallEmpty(db, "create (c:City{name:\"Made Up City\",url:\"/places/nowhere/made-up-city\"})", null);
        testCallCount(db, "call apoc.index.addAllNodesExtended('search_index',{City:['name']},{autoUpdate:true})", null, 1);
        indexUpdateTransactionEventHandler.forceTxRollover();

        // do delete immediately followed by a read
        db.execute("MATCH (c:City) DELETE c");
        testCallCount(db, "call apoc.index.search('search_index', 'City.name:Made') yield node, weight return node, weight", null, 0);
    }

}
