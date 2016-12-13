package apoc.result;

import org.neo4j.graphdb.Node;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * @author mh
 * @since 26.02.16
 */
public class MapNodeResult {
	private static final MapNodeResult EMPTY = new MapNodeResult(Collections.emptyMap());
	public final Map<String, Node> value;

	public static MapNodeResult empty() {
		return EMPTY;
	}

    public MapNodeResult(Map<String, Node> value) {
        this.value = value;
    }
}
