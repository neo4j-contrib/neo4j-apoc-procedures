package apoc.periodic;

import apoc.util.TransactionTestUtil;
import java.util.concurrent.TimeUnit;
import org.neo4j.common.DependencyResolver;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.api.KernelTransactions;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.rule.DbmsRule;

import java.util.Map;

import static org.junit.Assert.assertTrue;

public class PeriodicTestUtils {
    public static void killPeriodicQueryAsync(DbmsRule db) {
        new Thread(() -> {
            int retries = 10;
            try {
                while (retries-- > 0 && !terminateQuery("apoc.periodic", db)) {
                    Thread.sleep(10);
                }
            } catch (InterruptedException e) {
                // ignore
            }
        }).start();
    }

    public static boolean terminateQuery(String pattern, GraphDatabaseAPI db) {
        DependencyResolver dependencyResolver = db.getDependencyResolver();
        KernelTransactions kernelTransactions = dependencyResolver.resolveDependency(KernelTransactions.class);
        long numberOfKilledTransactions = kernelTransactions.activeTransactions().stream()
                .filter(kernelTransactionHandle ->
                        kernelTransactionHandle.executingQuery().map(query -> query.rawQueryText().contains(pattern))
                                .orElse(false)
                )
                .map(kernelTransactionHandle -> kernelTransactionHandle.markForTermination(Status.Transaction.Terminated))
                .count();
        return numberOfKilledTransactions > 0;
    }

    public static void testTerminatePeriodicQuery(DbmsRule db, String periodicQuery) {
        killPeriodicQueryAsync(db);
        checkPeriodicTerminated(db, periodicQuery);
    }

    public static void testTerminateWithCommand(DbmsRule db, String periodicQuery, String iterateQuery) {
        long timeBefore = System.currentTimeMillis();
        TransactionTestUtil.terminateTransactionAsync(db, 10L, iterateQuery);
        checkPeriodicTerminated(db, periodicQuery);
        TransactionTestUtil.lastTransactionChecks(db, periodicQuery, timeBefore);
    }

    private static void checkPeriodicTerminated(DbmsRule db, String periodicQuery) {
        try {
            org.neo4j.test.assertion.Assert.assertEventually( () ->
                            db.executeTransactionally(periodicQuery, Map.of(),
                                    result -> {
                                        Map<String, Object> row = Iterators.single(result);
                                        return (boolean) row.get("wasTerminated");
                                    }),
                    (value) -> value, 15L, TimeUnit.SECONDS);
        } catch(Exception tfe) {
            assertTrue(tfe.getMessage(), tfe.getMessage().contains("terminated"));
        }
    }
}
