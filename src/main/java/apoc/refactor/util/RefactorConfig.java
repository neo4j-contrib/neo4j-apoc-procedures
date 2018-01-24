package apoc.refactor.util;

import java.util.Collections;
import java.util.Map;

/**
 * @author AgileLARUS
 *
 * @since 20-06-17
 */
public class RefactorConfig {

	public static final String COMBINE = "combine";
	public static final String DISCARD = "discard";
	public static final String OVERWRITE = "overwrite";

	private static String MATCH_ALL = ".*";

	private Map<String,String> propertiesManagement = Collections.singletonMap(MATCH_ALL, OVERWRITE);

	public RefactorConfig(Map<String,Object> config) {
		Object value = config.get("properties");
		if (value instanceof String) {
			this.propertiesManagement = Collections.singletonMap(MATCH_ALL, value.toString());
		} else if (value instanceof Map) {
			this.propertiesManagement = (Map<String,String>)value;
		}
	}

	public String getMergeMode(String name){
		for (String key : propertiesManagement.keySet()) {
			if (!key.equals(MATCH_ALL) && name.matches(key)) {
				return propertiesManagement.get(key);
			}
		}
		return propertiesManagement.getOrDefault(name,propertiesManagement.getOrDefault(MATCH_ALL, OVERWRITE));

	}

}
