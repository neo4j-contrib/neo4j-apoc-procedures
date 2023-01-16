package apoc.result;

/**
 * @author mh
 * @since 26.02.16
 */
public class KeyValueResult {
	public final String key;
	public final Object value;

	public KeyValueResult(String key, Object value) {
		this.key = key;
		this.value = value;
	}
}
