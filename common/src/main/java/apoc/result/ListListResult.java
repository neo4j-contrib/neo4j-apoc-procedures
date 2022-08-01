package apoc.result;

import java.util.List;

/**
 * @author mh
 * @since 26.02.16
 */
public class ListListResult {
    public final List<List<Object>> value;

    public ListListResult(List<List<Object>> value) {
        this.value = value;
    }
}
