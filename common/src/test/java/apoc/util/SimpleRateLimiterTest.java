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
