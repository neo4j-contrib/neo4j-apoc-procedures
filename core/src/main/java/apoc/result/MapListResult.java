package apoc.result;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * @author mh
 * @since 26.02.16
 */
public class MapListResult {
	private static final MapListResult EMPTY = new MapListResult(Collections.emptyMap());
	public final Map<String, List<Object>> value;

	public static MapListResult empty() {
		return EMPTY;
	}

    public MapListResult(Map<String, List<Object>> value) {
        this.value = value;
    }
}
