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

import org.junit.Test;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static org.junit.Assert.*;

public class SimpleRateLimiterTest {

   @Test
   public void shouldTestTheRateLimiter() throws InterruptedException {
       // given
       SimpleRateLimiter srl = new SimpleRateLimiter(1000, 1);

       // when
       boolean canExecute = srl.canExecute();
       boolean cantExecute = srl.canExecute();
       Thread.sleep(2000);
       boolean nowCanExecute = srl.canExecute();

       // then
       assertTrue(canExecute);
       assertFalse(cantExecute);
       assertTrue(nowCanExecute);
   }

    @Test
    public void shouldTestTheRateLimiterInExecutors() throws InterruptedException {
        // given
        SimpleRateLimiter srl = new SimpleRateLimiter(1000, 10);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        Map<String, AtomicInteger> map = new ConcurrentHashMap<>();

        // when
        execRunnable(srl, executor, map, 50);
        execRunnable(srl, executor, map, 50);
        executor.awaitTermination(5, TimeUnit.SECONDS);

        // then
        int count = map
                .values()
                .stream()
                .map(AtomicInteger::get)
                .reduce(0, Integer::sum);
        assertEquals(10, count);
    }

    private void execRunnable(SimpleRateLimiter srl,
                              ExecutorService executor,
                              Map<String, AtomicInteger> map,
                              int operations) {
        executor.execute(() -> {
            AtomicInteger ai = map.computeIfAbsent(Thread.currentThread().getName(), k -> new AtomicInteger(0));
            IntStream.range(0, operations).forEach(i -> {
                if (srl.canExecute()) {
                    ai.incrementAndGet();
                }
            });
        });
    }

}
