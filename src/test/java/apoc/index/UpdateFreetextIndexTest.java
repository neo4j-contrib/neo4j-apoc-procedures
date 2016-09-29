package apoc.index;

import apoc.util.TestUtil;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.kernel.api.exceptions.KernelException;

import java.util.Arrays;
import java.util.Collection;

import static apoc.util.TestUtil.registerProcedure;
import static apoc.util.TestUtil.testResult;
import static org.junit.Assert.assertEquals;
import static org.neo4j.helpers.collection.Iterators.count;

/**
 * test to verify that fulltext indexes are updated, even across restarts of Neo4j
 * @author Stefan Armbruster
 */
@RunWith(Parameterized.class)
public class UpdateFreetextIndexTest {

    @Parameterized.Parameters(name = "with index config {0}, autoUpdate.enabled = {1}, async = {2}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                { "{}", true, false, false},
                { "{autoUpdate:false}", true, false, false},
                { "{autoUpdate:true}", true, false, true},
                { "{autoUpdate:true}", false, false, false},
                { "{}", true, true, false},
                { "{autoUpdate:false}", true, true, false},
                { "{autoUpdate:true}", true, true, true},
                { "{autoUpdate:true}", false, true, false}
        });
    }

    @Parameterized.Parameter(value = 0)
    public String paramIndexConfigMapAsString;

    @Parameterized.Parameter(value = 1)
    public boolean paramEnableAutoUpdatesInApocConfig;

    @Parameterized.Parameter(value = 2)
    public boolean paramDoUpdatesAsync;

    @Parameterized.Parameter(value = 3)
    public boolean paramExpectUpdates;

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    @Test
    public void shouldIndexGetUpdatedAcrossRestarts() throws KernelException, InterruptedException {

        // establish a graph db with a free text index
        GraphDatabaseService graphDatabaseService = initGraphDatabase();
        try {
            registerProcedure(graphDatabaseService, FreeTextSearch.class);
            graphDatabaseService.execute("create (:Article{text:'John owns a house in New York.'}) create (:Article{text:'Susan lives in a house together with John'})").close();
            Thread.sleep(1000);
            String indexingCypher = String.format("call apoc.index.addAllNodesExtended('fulltext',{Article:['text']},%s)", paramIndexConfigMapAsString);
            graphDatabaseService.execute(indexingCypher).close();

            // create another fulltext index
            indexingCypher = String.format("call apoc.index.addAllNodesExtended('fulltext2',{Article2:['text']},%s)", paramIndexConfigMapAsString);
            graphDatabaseService.execute(indexingCypher).close();

            TestUtil.testResult(graphDatabaseService, "call apoc.index.search('fulltext', 'house')", result ->
                assertEquals(2, count(result))
            );
            TestUtil.testResult(graphDatabaseService, "call apoc.index.search('fulltext2', 'house')", result ->
                    assertEquals(0, count(result))
            );
        } finally {
            graphDatabaseService.shutdown();
        }

        // restart graph db, add another node and check if it got indexed
        graphDatabaseService = initGraphDatabase();
        try {
            registerProcedure(graphDatabaseService, FreeTextSearch.class);
            graphDatabaseService.execute("create (:Article{text:'Mr. baker is renovating John\\'s house'}) ");

            if (paramDoUpdatesAsync) {
                Thread.sleep(300);
            }

            testResult(graphDatabaseService, "call apoc.index.search('fulltext', 'house')", result ->
                assertEquals(2 + (paramExpectUpdates ? 1 : 0), count(result))
            );

            graphDatabaseService.execute("match (n) set n:Article2");
            if (paramDoUpdatesAsync) {
                Thread.sleep(300);
            }
            testResult(graphDatabaseService, "call apoc.index.search('fulltext2', 'house')", result ->
                    assertEquals(paramExpectUpdates ? 3 : 0, count(result))
            );

        } finally {
            graphDatabaseService.shutdown();
        }

    }

    private GraphDatabaseService initGraphDatabase() {
        GraphDatabaseBuilder graphDatabaseBuilder = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(tmpFolder.getRoot());
        graphDatabaseBuilder.setConfig("apoc.autoIndex.enabled", Boolean.toString(paramEnableAutoUpdatesInApocConfig));
        graphDatabaseBuilder.setConfig("apoc.autoIndex.async", Boolean.toString(paramDoUpdatesAsync));
        graphDatabaseBuilder.setConfig("apoc.autoIndex.async_rollover_opscount", "10");
        graphDatabaseBuilder.setConfig("apoc.autoIndex.async_rollover_millis", "100");
        return graphDatabaseBuilder.newGraphDatabase();
    }

}
