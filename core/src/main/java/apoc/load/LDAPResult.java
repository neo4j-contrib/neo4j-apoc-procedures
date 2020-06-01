package apoc.load;

import java.util.Map;

public class LDAPResult {
    public final Map<String, Object> entry;

    public LDAPResult(Map<String, Object> entry) {
        this.entry = entry;
    }
}
