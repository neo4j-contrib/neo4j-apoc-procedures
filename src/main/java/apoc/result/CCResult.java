package apoc.result;

import java.util.List;
import java.util.Map;

/**
 * @author mh
 * @since 26.02.16
 */
public class CCResult {
    public final List<Long> nodeIds;
    public final Map<String, Long> stats;
	
    
    public CCResult(List<Long> nodeIds, Map stats) {
		this.nodeIds = nodeIds;
		this.stats = stats;
	}
    
}
