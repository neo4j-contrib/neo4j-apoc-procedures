package apoc;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.openjdk.jmh.annotations.Benchmark;

public class IndexUpdateBenchmarks {

    @Benchmark
    public void add10kNonIndexedNodes(GraphDatabaseState state) {
        final GraphDatabaseService db = state.getGraphDatabaseService();
        final Label label = Label.label("Person");
        try (Transaction tx = db.beginTx()) {
            for (int i=0; i<10000; i++) {
                Node n = db.createNode(label);
                n.setProperty("name", "myname_i");
            }
            tx.success();
        }

    }

    @Benchmark
    public void add10kIndexedNodes(IndexTrackingGraphDatabaseState state) {
//    public void add10kIndexedNodes(GraphDatabaseState state) throws KernelException {
//        TestUtil.registerProcedure(state.getGraphDatabaseService(), FreeTextSearch.class);
        final GraphDatabaseService db = state.getGraphDatabaseService();
        final Label label = Label.label("Person");
        try (Transaction tx = db.beginTx()) {
            for (int i=0; i<10000; i++) {
                Node n = db.createNode(label);
                n.setProperty("name", "myname_i");
            }
            tx.success();
        }

    }
}
