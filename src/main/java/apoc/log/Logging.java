package apoc.log;

import apoc.ApocConfiguration;
import apoc.util.SimpleRateLimiter;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.List;
import java.util.function.Consumer;

/**
 * @author bradnussbaum
 * @since 2017.07.28
 */
public class Logging {

    enum LoggingType {none, safe, raw}

    private static LoggingType LOGGING_TYPE;

    private static SimpleRateLimiter RATE_LIMITER;

    public Logging() {}

    public Logging(LoggingType loggingType, long timeWindow, int maxOpsPerWindow) { // for testing purpose only
        LOGGING_TYPE = loggingType;
        RATE_LIMITER = new SimpleRateLimiter(timeWindow, maxOpsPerWindow);

    }

    static {
        LOGGING_TYPE = LoggingType.valueOf(ApocConfiguration.get("user.log.type", "safe").trim());
        RATE_LIMITER = new SimpleRateLimiter(Long.valueOf(ApocConfiguration.get("user.log.window.time", "10000")),
                Integer.valueOf(ApocConfiguration.get("user.log.window.ops", "10")));
    }

    @Context
    public Log log;

    @Procedure
    @Description("apoc.log.error(message, params) - logs error message")
    public void error(@Name("message") String message,
                      @Name(value = "params", defaultValue = "[]") List<Object> params) {
        log((logMessage) -> log.error(logMessage), message, params);
    }

    @Procedure
    @Description("apoc.log.warn(message, params) - logs warn message")
    public void warn(@Name("message") String message,
                     @Name(value = "params", defaultValue = "[]") List<Object> params) {
        log((logMessage) -> log.warn(logMessage), message, params);
    }

    @Procedure
    @Description("apoc.log.info(message, params) - logs info message")
    public void info(@Name("message") String message,
                     @Name(value = "params", defaultValue = "[]") List<Object> params) {
        log((logMessage) -> log.info(logMessage), message, params);
    }

    @Procedure
    @Description("apoc.log.debug(message, params) - logs debug message")
    public void debug(@Name("message") String message,
                      @Name(value = "params", defaultValue = "[]") List<Object> params) {
        log((logMessage) -> log.debug(logMessage), message, params);
    }

    public String format(String message, List<Object> params) { // visible for testing
        if (canLog()) {
            String formattedMessage = String.format(message, params.isEmpty() ? new Object[0] : params.toArray(new Object[params.size()]));
            if (LoggingType.safe == LOGGING_TYPE) {
                return formattedMessage.replaceAll("\\.| |\\t", "_").toLowerCase();
            }
            return formattedMessage;
        }
        return null;
    }

    private void log(Consumer<String> consumer, String message, List<Object> params) {
        String format = format(message, params);
        if (format != null) {
            consumer.accept(format);
        }
    }

    private boolean canLog() {
        if (LoggingType.none == LOGGING_TYPE) {
            return false;
        }
        return RATE_LIMITER.canExecute();
    }

}
