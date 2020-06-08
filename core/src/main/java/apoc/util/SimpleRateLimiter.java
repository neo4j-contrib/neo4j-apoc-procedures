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
