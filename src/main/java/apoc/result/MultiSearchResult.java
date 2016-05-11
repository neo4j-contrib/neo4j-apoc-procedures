package apoc.result;

import java.util.List;
import java.util.Map;

public class MultiSearchResult {
	public List<String> label;
	public Map<String,Object> values;
	public long   id;

	public MultiSearchResult( List<String> labels,
			long id,
			Map<String,Object> val
			) {
		this.label = labels;
		this.id = id;
		this.values = val;
	}
	
}
