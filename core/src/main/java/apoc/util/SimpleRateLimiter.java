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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class SimpleRateLimiter {

    private final AtomicInteger countDownLatch = new AtomicInteger(0);
    private final AtomicLong lastUpdate = new AtomicLong(0);

    private final int timeWindow;
    private final int operationPerWindow;

    public SimpleRateLimiter(int timeWindow, int operationPerWindow) {
        this.timeWindow = timeWindow;
        this.operationPerWindow = operationPerWindow;
    }

    public synchronized boolean canExecute() {
        long now = System.currentTimeMillis();
        if ((now - lastUpdate.get()) > timeWindow) {
            lastUpdate.set(now);
            countDownLatch.set(operationPerWindow);
        }
        return countDownLatch.decrementAndGet() >= 0;
    }
}
