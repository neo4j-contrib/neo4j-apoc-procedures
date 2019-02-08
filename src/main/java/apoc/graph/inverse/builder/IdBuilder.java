package apoc.graph.inverse.builder;

import apoc.graph.inverse.util.Util;

import java.util.HashMap;
import java.util.Map;

public class IdBuilder {

    public static final String ID = "id";
    public static final String TYPE = "type";

    private Util id;
    private Util type;
    private Map<String, String> fields;

    public IdBuilder() { }

    public IdBuilder buildId(Map<String, Object> obj) {
        Object typeObj = obj.get(IdBuilder.TYPE);
        Object idObj = obj.get(IdBuilder.ID);

        if (idObj == null) {
            throw new RuntimeException("every object must have " + IdBuilder.ID + ": " + obj);
        } else if (typeObj == null) {
            throw new RuntimeException("every object must have " + IdBuilder.TYPE + ": " + obj);
        }

        return new IdBuilder(typeObj, idObj);
    }

    IdBuilder(Object type, Object id) {
        super();
        this.type = new Util(type);
        this.id = new Util(id);
        this.fields = new HashMap<>(2);
        this.fields.put(TYPE, this.type.toString());
        this.fields.put(ID, this.id.toString());
    }

    public Map<String, String> getFields() {
        return this.fields;
    }

    public String toCypherFilter() {
        return ID + ": " + this.id.toCypher() + ", " + TYPE + ": " + this.type.toCypher();
    }

}
