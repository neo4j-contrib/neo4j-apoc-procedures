package apoc.result;

import java.util.List;

/**
 * Created by alberto.delazzari on 04/07/17.
 */
public class IndexConstraintNodeInfo {

    public final String name;

    public final String label;

    public final List<String> properties;

    public final String status;

    public final String type;

    /**
     * Default constructor
     *
     * @param name
     * @param label
     * @param properties
     * @param status status of the index, if it's a constraint it will be empty
     * @param type if it is an index type will be "INDEX" otherwise it will be the type of constraint
     */
    public IndexConstraintNodeInfo(String name, String label, List<String> properties, String status, String type) {
        this.name = name;
        this.label = label;
        this.properties = properties;
        this.status = status;
        this.type = type;
    }
}
