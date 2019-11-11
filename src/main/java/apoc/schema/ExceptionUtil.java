package apoc.schema;

import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.exceptions.Status;

public final class ExceptionUtil {

    /**
     * Returns the root cause of an exception.
     *
     * Copied from {@link org.neo4j.helpers.Exceptions#rootCause(Throwable)} due to deprecation.
     *
     * @param caughtException exception to find the root cause of.
     * @return the root cause.
     * @throws IllegalArgumentException if the provided exception is null.
     */
    public static Throwable rootCause(Throwable caughtException) {
        if (null == caughtException) {
            throw new IllegalArgumentException("Cannot obtain rootCause from (null)");
        }
        Throwable root = caughtException;
        while (root.getCause() != null) {
            root = root.getCause();
        }
        return root;
    }

    /**
     * Adds the current exception to the initial exception as suppressed.
     *
     * Copied from {@link org.neo4j.helpers.Exceptions#chain(Throwable, Throwable)} due to deprecation.
     */
    public static <T extends Throwable> T chain(T initial, T current) {
        if (initial == null) {
            return current;
        }

        if (current != null) {
            initial.addSuppressed(current);
        }
        return initial;
    }

    public static RuntimeException asUnchecked(final Throwable exception) {
        if (exception instanceof RuntimeException) {
            return (RuntimeException) exception;
        }
        if (exception instanceof Error) {
            throw (Error) exception;
        }
        return new RuntimeException(exception);
    }

    public static <T> T throwKernelException(KernelException e) {
        Status status = e.status();
        String codeString = status.code().serialize();
        String message = e.getMessage();
        String newMessage;
        if (message == null || message.isEmpty()) {
            newMessage = codeString;
        } else {
            newMessage = codeString + ": " + message;
        }
        throw new RuntimeException(newMessage, e);
    }

    private ExceptionUtil() {
        throw new UnsupportedOperationException("No instances");
    }
}
