package apoc.util;

/**
 * Created by larusba on 5/18/17.
 */
public class MissingDependencyException extends RuntimeException{
    public MissingDependencyException(String message) {
        super(message);
    }
}
