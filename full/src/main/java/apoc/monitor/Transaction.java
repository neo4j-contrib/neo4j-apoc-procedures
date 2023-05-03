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
package apoc.monitor;

import apoc.Extended;
import apoc.result.TransactionInfoResult;
import org.neo4j.kernel.impl.transaction.stats.DatabaseTransactionStats;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Procedure;
import org.neo4j.storageengine.api.TransactionIdStore;

import java.util.stream.Stream;

@Extended
public class Transaction {

    @Context
    public GraphDatabaseAPI db;

    @Procedure
    @Description("apoc.monitor.tx() returns informations about the neo4j transaction manager")
    public Stream<TransactionInfoResult> tx() throws Exception {
        DatabaseTransactionStats stats = db.getDependencyResolver().resolveDependency(DatabaseTransactionStats.class);
        TransactionIdStore transactionIdStore = db.getDependencyResolver().resolveDependency(TransactionIdStore.class);
        return Stream.of(new TransactionInfoResult(
                stats.getNumberOfRolledBackTransactions(),
                stats.getPeakConcurrentNumberOfTransactions(),
                transactionIdStore.getLastCommittedTransactionId(),
                stats.getNumberOfActiveTransactions(),
                stats.getNumberOfStartedTransactions(),
                stats.getNumberOfCommittedTransactions()
        ));
    }

}
