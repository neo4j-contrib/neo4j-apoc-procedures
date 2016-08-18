package apoc.result;

import java.util.List;

/**
 * @author mh
 * @since 26.02.16
 */
public class ListListResult {
    public final List<List> value;

    public ListListResult(List<List> value) {
        this.value = value;
    }
}
