package apoc.monitor;

import apoc.result.TransactionInfoResult;
import org.neo4j.kernel.impl.transaction.stats.DatabaseTransactionStats;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Procedure;

import java.util.stream.Stream;

public class Transaction {

    @Context
    public GraphDatabaseAPI db;

    @Procedure
    @Description("apoc.monitor.tx() returns informations about the neo4j transaction manager")
    public Stream<TransactionInfoResult> tx() throws Exception {
        DatabaseTransactionStats stats = db.getDependencyResolver().resolveDependency(DatabaseTransactionStats.class);
        return Stream.of(new TransactionInfoResult(
                stats.getNumberOfRolledBackTransactions(),
                stats.getPeakConcurrentNumberOfTransactions(),
                stats.getNumberOfStartedTransactions(),
                stats.getNumberOfActiveTransactions(),
                stats.getNumberOfStartedTransactions(),
                stats.getNumberOfCommittedTransactions()
        ));
    }

}
