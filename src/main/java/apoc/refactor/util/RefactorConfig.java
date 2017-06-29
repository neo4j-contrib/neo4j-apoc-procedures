package apoc.refactor.util;

import java.util.Map;

/**
 * @author AgileLARUS
 *
 * @since 20-06-17
 */
public class RefactorConfig {

	private String propertiesManagement;

	public RefactorConfig(Map<String,Object> config) {
		this.propertiesManagement = (String) config.getOrDefault("properties", "overwrite");
	}

	public String getPropertiesManagement(){ return propertiesManagement; }

}
