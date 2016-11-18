package apoc.result;

import java.util.List;
import java.util.Map;

/**
 * @author mh
 * @since 26.02.16
 */
public class MapListResult {
    public final List<Map> maps;

    public MapListResult(List<Map> maps) {
        this.maps = maps;
    }
}
