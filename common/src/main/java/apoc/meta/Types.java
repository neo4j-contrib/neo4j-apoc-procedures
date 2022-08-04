package apoc.meta;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.spatial.Point;
import org.neo4j.values.storable.DurationValue;

    
public enum Types {
    INTEGER,FLOAT,STRING,BOOLEAN,RELATIONSHIP,NODE,PATH,NULL,ANY,MAP,LIST,POINT,DATE,DATE_TIME,LOCAL_TIME,LOCAL_DATE_TIME,TIME,DURATION;

    private String typeOfList = "ANY";

    private static Map<Class<?>, Class<?>> primitivesMapping = new HashMap(){{
        put(double.class, Double.class);
        put(float.class, Float.class);
        put(int.class, Integer.class);
        put(long.class, Long.class);
        put(short.class, Short.class);
        put(boolean.class, Boolean.class);
    }};

    @Override
    public String toString() {
        switch (this){
            case LIST:
                return "LIST OF " + typeOfList;
            default:
                return super.toString();
        }
    }

    public static Types of(Object value) {
        Types type = of(value == null ? null : value.getClass());
        if (type == Types.LIST && !value.getClass().isArray()) {
            type.typeOfList = inferType((List<?>) value);
        }
        return type;
    }

    public static Types of(Class<?> type) {
        if (type==null) return NULL;
        if (type.isArray()) {
            Types innerType = Types.of(type.getComponentType());
            Types returnType = LIST;
            returnType.typeOfList = innerType.toString();
            return returnType;
        }
        if (type.isPrimitive()) { type = primitivesMapping.getOrDefault(type, type); }
        if (Number.class.isAssignableFrom(type)) { return Double.class.isAssignableFrom(type) || Float.class.isAssignableFrom(type) ? FLOAT : INTEGER; }
        if (Boolean.class.isAssignableFrom(type)) { return BOOLEAN; }
        if (String.class.isAssignableFrom(type)) { return STRING; }
        if (Map.class.isAssignableFrom(type)) { return MAP; }
        if (Node.class.isAssignableFrom(type)) { return NODE; }
        if (Relationship.class.isAssignableFrom(type)) { return RELATIONSHIP; }
        if (Path.class.isAssignableFrom(type)) { return PATH; }
        if (Point.class.isAssignableFrom(type)){ return POINT; }
        if (List.class.isAssignableFrom(type)) { return LIST; }
        if (LocalDate.class.isAssignableFrom(type)) { return DATE; }
        if (LocalTime.class.isAssignableFrom(type)) { return LOCAL_TIME; }
        if (LocalDateTime.class.isAssignableFrom(type)) { return LOCAL_DATE_TIME; }
        if (DurationValue.class.isAssignableFrom(type)) { return DURATION; }
        if (OffsetTime.class.isAssignableFrom(type)) { return TIME; }
        if (ZonedDateTime.class.isAssignableFrom(type)) { return DATE_TIME; }
        return ANY;
    }

    public static Types from(String typeName) {
        if (typeName == null) {
            return STRING;
        }
        typeName = typeName.toUpperCase().replace("_", "");
        for (Types type : values()) {
            final String name = type.name().replace("_", "");
            // check, e.g. both "LOCAL_DATE_TIME" and "LOCALDATETIME"
            if (name.startsWith(typeName)) {
                return type;
            }
        }
        return STRING;
    }

    public static String inferType(List<?> list) {
        Set<String> set = list.stream().limit(10).map(e -> of(e).name()).collect(Collectors.toSet());
        return set.size() != 1 ? "ANY" : set.iterator().next();
    }
}
