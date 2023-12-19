package apoc.result;

import java.util.List;

public abstract class IndexConstraintEntityInfo {
    public final String name;
    public final String type;
    public final List<String> properties;

    public IndexConstraintEntityInfo(String name, List<String> properties, String type) {
        this.name = name;
        this.properties = properties;
        this.type = type;
    }
}
