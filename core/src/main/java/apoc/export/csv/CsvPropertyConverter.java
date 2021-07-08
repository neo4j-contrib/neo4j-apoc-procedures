package apoc.export.csv;

import org.neo4j.graphdb.Entity;

import java.util.List;

public class CsvPropertyConverter {

    public static boolean addPropertyToGraphEntity(Entity entity, CsvHeaderField field, Object value) {
        if (field.isIgnore() || value == null) {
            return false;
        }
        if (field.isArray()) {
            final Object[] prototype = getPrototypeFor(field.getType());
            final Object[] array = ((List<Object>) value).toArray(prototype);
            entity.setProperty(field.getName(), array);
        } else {
            entity.setProperty(field.getName(), value);
        }
        return true;
    }

    static Object[] getPrototypeFor(String type) {
        switch (type) {
            case "INT":     return new Integer  [] {};
            case "LONG":    return new Long     [] {};
            case "FLOAT":   return new Float    [] {};
            case "DOUBLE":  return new Double   [] {};
            case "BOOLEAN": return new Boolean  [] {};
            case "BYTE":    return new Byte     [] {};
            case "SHORT":   return new Short    [] {};
            case "CHAR":    return new Character[] {};
            case "STRING":  return new String   [] {};
        }
        throw new IllegalStateException("Type " + type + " not supported.");
    }

}
