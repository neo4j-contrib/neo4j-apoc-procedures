package apoc.generate.config;

/**
 * Exception signifying an invalid config used for graph generation.
 */
public class InvalidConfigException extends RuntimeException {

    public InvalidConfigException(String message) {
        super(message);
    }
}
