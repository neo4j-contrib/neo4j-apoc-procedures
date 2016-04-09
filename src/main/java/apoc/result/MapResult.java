package apoc.result;

import java.util.Collections;
import java.util.Map;

/**
 * @author mh
 * @since 26.02.16
 */
public class MapResult {
	private static final MapResult EMPTY = new MapResult(Collections.emptyMap());
	public final Map value;

	public static MapResult empty() {
		return EMPTY;
	}

    public MapResult(Map value) {
        this.value = value;
    }
}
