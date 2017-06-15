package apoc;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.openjdk.jmh.annotations.Benchmark;

public class IndexUpdateBenchmarks {

    @Benchmark
    public void add10kNonIndexedNodes(GraphDatabaseState state) {
        populateDb( state, 10000 );
    }

    @Benchmark
    public void add10kNodesWithBackgroundUpdatesEnabled(IndexTrackingGraphDatabaseState state) {
        populateDb( state, 10000 );
    }

    @Benchmark
    public void add10kIndexedSyncNodes(SyncIndexingGraphDatabaseState state) {
        populateDb( state, 10000 );
    }

    @Benchmark
    public void add10kIndexedAsyncNodes(AsyncIndexingGraphDatabaseState state) {
        populateDb( state, 10000 );
    }

    private void populateDb( GraphDatabaseState state, int numberOfNodes )
    {
        final GraphDatabaseService db = state.getGraphDatabaseService();
        final Label label = Label.label("Person");
        try (Transaction tx = db.beginTx()) {
            for (int i=0; i<numberOfNodes; i++) {
                Node n = db.createNode(label);
                n.setProperty("name", "myname_i");
            }
            tx.success();
        }
    }

}
