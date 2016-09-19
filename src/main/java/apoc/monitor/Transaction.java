package apoc.monitor;

import org.neo4j.procedure.Description;
import apoc.result.TransactionInfoResult;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Procedure;

import javax.management.ObjectName;
import java.util.stream.Stream;

import static org.neo4j.jmx.JmxUtils.getAttribute;
import static org.neo4j.jmx.JmxUtils.getObjectName;

public class Transaction {

    private static final String JMX_OBJECT_NAME = "Transactions";
    private static final String ROLLED_BACK_TX = "NumberOfRolledBackTransactions";
    private static final String PEAK_TX = "PeakNumberOfConcurrentTransactions";
    private static final String LAST_TX_ID = "LastCommittedTxId";
    private static final String OPEN_TX = "NumberOfOpenTransactions";
    private static final String OPENED_TX = "NumberOfOpenedTransactions";
    private static final String COMMITTED_TX = "NumberOfCommittedTransactions";

    @Context
    public GraphDatabaseService db;

    @Procedure
    @Description("apoc.monitor.tx() returns informations about the neo4j transaction manager")
    public Stream<TransactionInfoResult> tx() throws Exception {
        return Stream.of(getTransactionInfo());
    }

    private TransactionInfoResult getTransactionInfo() throws Exception {
        ObjectName objectName = getObjectName(db, JMX_OBJECT_NAME);

        return new TransactionInfoResult(
                getAttribute(objectName, ROLLED_BACK_TX),
                getAttribute(objectName, PEAK_TX),
                getAttribute(objectName, LAST_TX_ID),
                getAttribute(objectName, OPEN_TX),
                getAttribute(objectName, OPENED_TX),
                getAttribute(objectName, COMMITTED_TX)
        );
    }



}
