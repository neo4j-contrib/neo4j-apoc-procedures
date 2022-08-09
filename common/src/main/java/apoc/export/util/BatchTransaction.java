package apoc.export.util;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;

/**
* @author mh
* @since 16.01.14
*/
public class BatchTransaction implements AutoCloseable {
    private final GraphDatabaseService gdb;
    private final int batchSize;
    private final Reporter reporter;
    Transaction tx;
    int count = 0;
    int batchCount = 0;

    public BatchTransaction(GraphDatabaseService gdb, int batchSize, Reporter reporter) {
        this.gdb = gdb;
        this.batchSize = batchSize;
        this.reporter = reporter;
        tx = beginTx();
    }

    public void increment() {
        count++;batchCount++;
        if (batchCount >= batchSize) {
            doCommit(true);
        }
    }

    public void commit() {
        doCommit(true);
    }

    public int getCount() {
        return count;
    }

    public void manualCommit(boolean log) {
        doCommit(log);
    }

    private void doCommit(boolean log) {
        tx.commit();
        tx.close();
        if (log && reporter!=null) reporter.progress("commit after " + count + " row(s) ");
        tx = beginTx();
        batchCount = 0;
    }

    private Transaction beginTx() {
        return gdb.beginTx();
    }

    @Override
    public void close() {
        if (tx!=null) {
            tx.commit();
            tx.close();
            if (reporter!=null) reporter.progress("finish after " + count + " row(s) ");
        }
    }

    public Transaction getTransaction() {
        return tx;
    }
}
