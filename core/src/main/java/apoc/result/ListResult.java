package apoc.result;

import java.util.List;

/**
 * @author mh
 * @since 26.02.16
 */
public class ListResult {
    public final List<Object> value;

    public ListResult(List<Object> value) {
        this.value = value;
    }
}
