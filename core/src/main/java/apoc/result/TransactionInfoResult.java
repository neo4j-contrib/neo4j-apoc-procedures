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
package apoc.result;

public class TransactionInfoResult {

    public long rolledBackTx;

    public long peakTx;

    public long lastTxId;

    public long currentOpenedTx;

    public long totalOpenedTx;

    public long totalTx;

    public TransactionInfoResult(
            long rolledBackTx,
            long peakTx,
            long lastTxId,
            long currentOpenedTx,
            long totalOpenedTx,
            long totalTx
    ) {
        this.rolledBackTx = rolledBackTx;
        this.peakTx = peakTx;
        this.lastTxId = lastTxId;
        this.currentOpenedTx = currentOpenedTx;
        this.totalOpenedTx = totalOpenedTx;
        this.totalTx = totalTx;
    }

}
