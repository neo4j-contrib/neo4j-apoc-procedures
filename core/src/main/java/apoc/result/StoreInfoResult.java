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

public class StoreInfoResult {

    public long logSize;

    public long stringStoreSize;

    public long arrayStoreSize;

    public long relStoreSize;

    public long propStoreSize;

    public long totalStoreSize;

    public long nodeStoreSize;

    public StoreInfoResult(
            long logSize,
            long stringStoreSize,
            long arrayStoreSize,
            long relStoreSize,
            long propStoreSize,
            long totalStoreSize,
            long nodeStoreSize
    ) {
        this.logSize = logSize;
        this.stringStoreSize = stringStoreSize;
        this.arrayStoreSize = arrayStoreSize;
        this.relStoreSize = relStoreSize;
        this.propStoreSize = propStoreSize;
        this.totalStoreSize = totalStoreSize;
        this.nodeStoreSize = nodeStoreSize;
    }

}
