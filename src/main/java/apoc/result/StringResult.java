package apoc.result;

/**
 * @author mh
 * @since 26.02.16
 */
public class StringResult {
    public final static StringResult EMPTY = new StringResult(null);

    public final String value;

    public StringResult(String value) {
        this.value = value;
    }
}
