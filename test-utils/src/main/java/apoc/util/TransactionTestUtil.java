/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package apoc.util;

import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testResult;
import static java.util.Collections.emptyMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.test.assertion.Assert.assertEventually;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;

public class TransactionTestUtil {
    public static final String TRANSACTION_LIST = "SHOW TRANSACTIONS";
    public static final long DEFAULT_TIMEOUT = 10L;

    public static void checkTerminationGuard(GraphDatabaseService db, String query) {
        checkTerminationGuard(db, query, emptyMap());
    }

    public static void checkTerminationGuard(GraphDatabaseService db, long timeout, String query) {
        checkTerminationGuard(db, timeout, query, emptyMap());
    }

    public static void checkTerminationGuard(GraphDatabaseService db, String query, Map<String, Object> params) {
        checkTerminationGuard(db, DEFAULT_TIMEOUT, query, params);
    }

    public static void checkTerminationGuard(
            GraphDatabaseService db, long timeout, String query, Map<String, Object> params) {
        terminateTransactionAsync(db, timeout, query);

        // check that the procedure/function fails with TransactionFailureException when transaction is terminated
        long timePassed = System.currentTimeMillis();
        try (Transaction transaction = db.beginTx(timeout, TimeUnit.SECONDS)) {
            transaction.execute(query, params).resultAsString();
            transaction.commit();
            fail("Should fail because of TransactionFailureException");
        } catch (Exception e) {
            final String msg = e.getMessage();
            assertTrue(
                    "Actual message is: " + msg,
                    Stream.of("terminated", "failed", "closed").anyMatch(msg::contains));
        }

        lastTransactionChecks(db, timeout, query, timePassed);
    }

    public static void lastTransactionChecks(GraphDatabaseService db, long timeout, String query, long timePassed) {
        checkTransactionTime(timeout, timePassed);
        checkTransactionNotInList(db, query);
    }

    public static void lastTransactionChecks(GraphDatabaseService db, String query, long timeBefore) {
        lastTransactionChecks(db, DEFAULT_TIMEOUT, query, timeBefore);
    }

    public static void checkTransactionTime(long timeout, long timePassed) {
        timePassed = (System.currentTimeMillis() - timePassed) / 1000;
        assertTrue(
                "The transaction hasn't been terminated before the timeout time, but after " + timePassed + " seconds",
                timePassed <= timeout);
    }

    public static void checkTransactionNotInList(GraphDatabaseService db, String query) {
        // checking for query cancellation from transaction list command
        testResult(db, TRANSACTION_LIST, Map.of("query", query), result -> {
            final boolean currentQuery =
                    result.columnAs("currentQuery").stream().noneMatch(currQuery -> currQuery.equals(query));
            assertTrue(currentQuery);
        });
    }

    public static void terminateTransactionAsync(GraphDatabaseService db, String query) {
        terminateTransactionAsync(db, DEFAULT_TIMEOUT, query);
    }

    public static void terminateTransactionAsync(GraphDatabaseService db, long timeout, String query) {
        new Thread(() -> {
                    // waiting for apoc query to cancel when it is found
                    final String[] transactionId = new String[1];

                    assertEventually(
                            () -> db.executeTransactionally(
                                    TRANSACTION_LIST + " YIELD currentQuery, transactionId "
                                            + "WHERE currentQuery CONTAINS $query AND NOT currentQuery STARTS WITH $transactionList "
                                            + "RETURN transactionId",
                                    Map.of("query", query, "transactionList", TRANSACTION_LIST),
                                    result -> {
                                        final ResourceIterator<String> msgIterator = result.columnAs("transactionId");
                                        if (!msgIterator.hasNext()) {
                                            return false;
                                        }
                                        transactionId[0] = msgIterator.next();
                                        assertNotNull(transactionId[0]);

                                        // sometimes `TERMINATE TRANSACTION $transactionId` fails with `Transaction not
                                        // found`
                                        // even if has been retrieved by `SHOW TRANSACTIONS`
                                        testCall(
                                                db,
                                                "TERMINATE TRANSACTION $transactionId",
                                                Map.of("transactionId", transactionId[0]),
                                                r1 -> assertEquals("Transaction terminated.", r1.get("message")));

                                        return true;
                                    }),
                            (value) -> value,
                            timeout,
                            TimeUnit.SECONDS);
                })
                .start();
    }
}
