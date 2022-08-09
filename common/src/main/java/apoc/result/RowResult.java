package apoc.result;

import java.util.Map;

/**
 * @author mh
 * @since 26.02.16
 */
public class RowResult {
    public final Map<String, Object> row;

    public RowResult(Map<String, Object> row) {
        this.row = row;
    }
}
