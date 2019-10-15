package apoc.atomic;

import org.neo4j.graphdb.Lock;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.time.Instant;

public class Dummy {

    @Context
    public Transaction transaction;

    @Context
    public KernelTransaction ktx;

    @Procedure
    public void checkTransactionIdentities(@Name("id") long id) throws InterruptedException {
        log("acquiring lock");
        Lock lock = transaction.acquireWriteLock(transaction.getNodeById(id));
        log("got lock - do work");
        Thread.sleep(1000);
        log("return from proc");
    }

    public static void log(String message, Object... params) {
        System.out.println(Thread.currentThread().getName() + " " + Instant.now() + " " + String.format(message, params));
    }

}
