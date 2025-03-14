package apoc.util;

import static org.junit.Assert.*;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class ExtendedUtilTest {

    private static int i = 0;

    @Test
    public void testWithLinearBackOffRetriesWithSuccess() {
        i = 0;
        long start = System.currentTimeMillis();
        int result = ExtendedUtil.withBackOffRetries(
                this::testFunction,
                true,
                -1, // test backoffRetry default value -> 5
                false,
                runEx -> {
                    if (!runEx.getMessage().contains("Expected")) throw new RuntimeException("Some Bad News...");
                });
        long time = System.currentTimeMillis() - start;

        assertEquals(4, result);

        // The method will attempt to execute the operation with a linear backoff strategy,
        // sleeping for 1 second, 2 seconds, and 3 seconds between retries.
        // This results in a total wait time of 6 seconds (1s + 2s + 3s + 4s) if the operation succeeds on the third
        // attempt,
        // leading to an approximate execution time of 6 seconds.
        assertTrue("Current time is: " + time, time > 9000 && time < 11000);
    }

    @Test
    public void testWithExponentialBackOffRetriesWithSuccess() {
        i = 0;
        long start = System.currentTimeMillis();
        int result = ExtendedUtil.withBackOffRetries(
                this::testFunction,
                true,
                0, // test backoffRetry default value -> 5
                true,
                runEx -> {});
        long time = System.currentTimeMillis() - start;

        assertEquals(4, result);

        // The method will attempt to execute the operation with an exponential backoff strategy,
        // sleeping for 2 second, 4 seconds, and 8 seconds between retries.
        // This results in a total wait time of 30 seconds (2s + 4s + 8s + 16s) if the operation succeeds on the third
        // attempt,
        // leading to an approximate execution time of 14 seconds.
        assertTrue("Current time is: " + time, time > 29000 && time < 31000);
    }

    @Test
    public void testBackOffRetriesWithError() {
        i = 0;
        long start = System.currentTimeMillis();
        assertThrows(
                RuntimeException.class,
                () -> ExtendedUtil.withBackOffRetries(this::testFunction, true, 2, false, runEx -> {}));
        long time = System.currentTimeMillis() - start;

        // The method is configured to retry the operation twice.
        // So, it will make two extra-attempts, waiting for 1 second and 2 seconds before failing and throwing an
        // exception.
        // Resulting in an approximate execution time of 3 seconds.
        assertTrue("Current time is: " + time, time > 2000 && time < 4000);
    }

    @Test
    public void testBackOffRetriesWithErrorAndExponential() {
        i = 0;
        long start = System.currentTimeMillis();
        assertThrows(
                RuntimeException.class,
                () -> ExtendedUtil.withBackOffRetries(this::testFunction, true, 2, true, runEx -> {}));
        long time = System.currentTimeMillis() - start;

        // The method is configured to retry the operation twice.
        // So, it will make two extra-attempts, waiting for 2 second and 4 seconds before failing and throwing an
        // exception.
        // Resulting in an approximate execution time of 6 seconds.
        assertTrue("Current time is: " + time, time > 5000 && time < 7000);
    }

    @Test
    public void testWithoutBackOffRetriesWithError() {
        i = 0;
        assertThrows(
                RuntimeException.class,
                () -> ExtendedUtil.withBackOffRetries(this::testFunction, false, 30, false, runEx -> {}));

        // Retry strategy is not active and the testFunction is executed only once by raising an exception.
        assertEquals(1, i);
    }

    private int testFunction() {
        if (i == 4) {
            return i;
        }
        i++;
        throw new RuntimeException("Expected i not equal to 4");
    }
}
