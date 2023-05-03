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

    public void rollback() {
        tx.rollback();
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
            tx.close();
            if (reporter!=null) reporter.progress("finish after " + count + " row(s) ");
        }
    }

    public Transaction getTransaction() {
        return tx;
    }
}
