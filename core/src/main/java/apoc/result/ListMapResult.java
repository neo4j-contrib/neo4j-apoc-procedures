package apoc.result;

import java.util.List;
import java.util.Map;

/**
 * @author mh
 * @since 26.02.16
 */
public class ListMapResult {
    public final List<Map<String,Object>> maps;

    public ListMapResult(List<Map<String,Object>> maps) {
        this.maps = maps;
    }
}
