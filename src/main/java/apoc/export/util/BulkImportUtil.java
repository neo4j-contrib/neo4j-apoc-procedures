package apoc.export.util;

import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.PointValue;

import java.time.*;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class BulkImportUtil {

    private static Map<Class<?>, String> allowedMapping = Collections.unmodifiableMap(new HashMap(){{
        put(Double.class, "double");
        put(Float.class, "float");
        put(Integer.class, "int");
        put(Long.class, "long");
        put(Short.class, "short");
        put(Character.class, "char");
        put(Byte.class, "byte");
        put(Boolean.class, "boolean");
        put(DurationValue.class, "duration");
        put(PointValue.class, "point");
        put(LocalDate.class, "date");
        put(LocalDateTime.class, "localdatetime");
        put(LocalTime.class, "localtime");
        put(ZonedDateTime.class, "datetime");
        put(OffsetTime.class, "time");
    }});


    public static String formatHeader(Map.Entry<String, Class> r) {
        if (allowedMapping.containsKey(r.getValue())) {
            return r.getKey() + ":" + allowedMapping.get(r.getValue());
        } else {
            return r.getKey();
        }
    }

}
