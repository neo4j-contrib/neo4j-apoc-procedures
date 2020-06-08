package apoc.help;

import java.util.List;
import java.util.Map;

/**
 * @author mh
 * @since 11.04.16
 */
public class HelpResult {
    public String type;
    public String name;
    public String text;
    public String signature;
    public List<String> roles;
    public Boolean writes;
    public Boolean core;

    public HelpResult(String type, String name, String text, String signature, List<String> roles, Boolean writes, Boolean core) {
        this.type = type;
        this.name = name;
        this.text = text;
        this.signature = signature;
        this.roles = roles;
        this.writes = writes;
        this.core = core;
    }

    public HelpResult(Map<String, Object> row, Boolean core) {
        this((String)row.get("type"),(String)row.get("name"),(String)row.get("description"),(String)row.get("signature"),null,(Boolean)row.get("writes"), core);
    }
}
