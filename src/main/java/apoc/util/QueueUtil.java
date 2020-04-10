package apoc.util;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class QueueUtil {

    public static final int WAIT = 1;
    public static final TimeUnit WAIT_UNIT = TimeUnit.SECONDS;

    public static <T> void put(BlockingQueue<T> queue, T item, long timeoutSeconds) {
        put(queue, item, timeoutSeconds, true, () -> {});
    }

    /**
     * to be used instead of {@link BlockingQueue#put}
     * @param queue
     * @param item
     * @param timeoutSeconds
     * @param failWithExecption true if a {@link RuntimeException} should be thrown in case we couldn't add item into the queue within timeframe
     * @param checkDuringOffering a callback supposed to throw an exception to terminate
     * @param <T>
     */
    public static <T> void put(BlockingQueue<T> queue, T item, long timeoutSeconds, boolean failWithExecption, Runnable checkDuringOffering) {
        try {
            long timeoutTimestamp = System.currentTimeMillis() + timeoutSeconds * 1000;
            while (true) {
                if (System.currentTimeMillis() > timeoutTimestamp) break;
                boolean success = queue.offer(item, WAIT, WAIT_UNIT);
                if (success) {
                    return;
                }
                checkDuringOffering.run();
            }
            if (failWithExecption) {
                throw new RuntimeException("Error queuing item before timeout of " + timeoutSeconds + " seconds");
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * to be used instead of {@link BlockingQueue#take}
     * @param queue
     * @param timeoutSeconds
     * @param checkDuringPolling a callback supposed to throw an exception to terminate
     * @param <T>
     * @return
     */
    public static <T> T take(BlockingQueue<T> queue, long timeoutSeconds, Runnable checkDuringPolling) {
        try {
            long started = System.currentTimeMillis();
            while (started + timeoutSeconds * 1000 > System.currentTimeMillis()) {
                T polled = queue.poll(WAIT, WAIT_UNIT);
                if (polled != null) {
                    return polled;
                }
                checkDuringPolling.run();
            }
            throw new RuntimeException("Error polling, timeout of " + timeoutSeconds + " seconds reached.");
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

}
