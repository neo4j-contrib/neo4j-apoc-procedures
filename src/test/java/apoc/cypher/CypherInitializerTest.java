package apoc.cypher;

import org.junit.After;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.test.TestGraphDatabaseFactory;

import static apoc.util.TestUtil.testResult;
import static org.junit.Assert.assertEquals;

public class CypherInitializerTest {

    public GraphDatabaseService db ;

    public void init(String... initializers) {
        GraphDatabaseBuilder graphDatabaseBuilder = new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder();

        int index = 1;
        for (String initializer: initializers) {
            graphDatabaseBuilder.setConfig("apoc.initializer.cypher." + index++, initializer);
        }
        db = graphDatabaseBuilder.newGraphDatabase();
    }

    @After
    public void teardown() {
        db.shutdown();
    }

    @Test
    public void emptyInitializerWorks() {
        init("");
        expectNodeCount(0);
    }

    @Test
    public void singleInitializerWorks() {
        init("create()");
        expectNodeCount(1);
    }

    @Test
    public void multipleInitializersWorks() {
        init("create ()", "match (n) create ()");  // this only creates 2 nodes if the statements run in same order
        expectNodeCount(2);
    }

    @Test
    public void multipleInitializersWorks2() {
        init("match (n) create ()", "create ()");
        expectNodeCount(1);
    }

    private void expectNodeCount(int i) {
        testResult(db, "match (n) return n", result -> assertEquals(i, Iterators.count(result)));
    }

}
