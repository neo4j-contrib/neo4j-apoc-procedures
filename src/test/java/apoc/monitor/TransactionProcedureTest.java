package apoc.monitor;

import org.junit.Ignore;
import org.junit.Test;

import javax.management.ObjectName;

import static apoc.util.TestUtil.testCall;
import static org.junit.Assert.*;

@Ignore
public class TransactionProcedureTest extends MonitorTestCase {

    @Override
    Class procedureClass() {
        return Transaction.class;
    }

    // These tests fails with an impermanent db

//
//    @Test
//    public void testGetTransactionInfo() {
//        createData();
//        ObjectName objectName = JmxUtils.getObjectName(db, "Transactions");
//        long tx = (long) JmxUtils.getAttribute(objectName, "NumberOfRolledBackTransactions");
//        System.out.println(tx);
//        testCall(db, "CALL apoc.monitor.tx()", (row) -> {
//            assertEquals(0, (long) row.get("rolledBackTx"));
//        });
//    }
//
//    private void createData() {
//        testCall(db, "CREATE (n)", (row) -> {});
//    }
}
