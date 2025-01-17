package apoc.util;

import java.time.Duration;
import java.util.Collection;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.commons.collections4.CollectionUtils;

public class ExtendedUtil {
    public static <T> T withBackOffRetries(
            Supplier<T> func,
            boolean retry,
            int backoffRetry,
            boolean exponential,
            Consumer<Exception> exceptionHandler) {
        T result;
        backoffRetry = backoffRetry < 1 ? 5 : backoffRetry;
        int countDown = backoffRetry;
        exceptionHandler = Objects.requireNonNullElse(exceptionHandler, exe -> {});
        while (true) {
            try {
                result = func.get();
                break;
            } catch (Exception e) {
                if (!retry || countDown < 1) throw e;
                exceptionHandler.accept(e);
                countDown--;
                long delay = getDelay(backoffRetry, countDown, exponential);
                backoffSleep(delay);
            }
        }
        return result;
    }

    private static void backoffSleep(long millis) {
        sleep(millis, "Operation interrupted during backoff");
    }

    public static void sleep(long millis, String interruptedMessage) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(interruptedMessage, ie);
        }
    }

    private static long getDelay(int backoffRetry, int countDown, boolean exponential) {
        int backOffTime = backoffRetry - countDown;
        long sleepMultiplier = exponential
                ? (long) Math.pow(2, backOffTime)
                : // Exponential retry progression
                backOffTime; // Linear retry progression
        return Math.min(
                Duration.ofSeconds(1).multipliedBy(sleepMultiplier).toMillis(),
                Duration.ofSeconds(30).toMillis() // Max 30s
                );
    }

    public static String joinStringLabels(Collection<String> labels) {
        return CollectionUtils.isNotEmpty(labels)
                ? ":" + labels.stream().map(Util::quote).collect(Collectors.joining(":"))
                : "";
    }
}
