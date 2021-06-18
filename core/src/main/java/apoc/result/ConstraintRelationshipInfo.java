package apoc.result;

import java.util.List;

/**
 * Created by alberto.delazzari on 04/07/17.
 */
public class ConstraintRelationshipInfo {

    public final String name;

    public final String type;

    public final List<String> properties;

    public final String status;

    public ConstraintRelationshipInfo(String name, String type, List<String> properties, String status) {
        this.name = name;
        this.type = type;
        this.properties = properties;
        this.status = status;
    }
}
