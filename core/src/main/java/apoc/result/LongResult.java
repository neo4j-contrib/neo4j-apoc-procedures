package apoc.result;

import java.util.stream.Stream;

/**
 * @author mh
 * @since 26.02.16
 */
public class LongResult {
    public static final LongResult NULL = new LongResult(null);
    public final Long value;

    public LongResult(Long value) {
        this.value = value;
    }
}
