package apoc.export.csv;

import org.apache.commons.lang3.StringUtils;
import org.neo4j.graphdb.Entity;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.PointValue;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Objects;

public class CsvPropertyConverter {

    public static boolean addPropertyToGraphEntity(Entity entity, CsvHeaderField field, Object value, CsvLoaderConfig config) {
        if (field.isIgnore() || value == null) {
            return false;
        }
        if (field.isArray()) {
            final List list = (List) value;
            final boolean listContainingNull = list.stream().anyMatch(Objects::isNull);
            if (listContainingNull) {
                return false;
            }
            final Object[] prototype = getPrototypeFor(field.getType().toUpperCase());
            final Object[] array = list.toArray(prototype);
            entity.setProperty(field.getName(), array);
        } else {
            if (config.isIgnoreBlankString() && value instanceof String && StringUtils.isBlank((String) value)) {
                return false;
            }
            entity.setProperty(field.getName(), value);
        }
        return true;
    }

    static Object[] getPrototypeFor(String type) {
        switch (type) {
            case "INT":
            case "LONG":
                return new Long[] {};
            case "FLOAT":
            case "DOUBLE":
                return new Double[] {};
            case "BOOLEAN":
                return new Boolean[] {};
            case "BYTE":
                return new Byte[] {};
            case "SHORT":
                return new Short[] {};
            case "CHAR":
                return new Character[] {};
            case "STRING":
                return new String[] {};
            case "DATETIME":
                return new ZonedDateTime[] {};
            case "LOCALTIME":
                return new LocalTime[] {};
            case "LOCALDATETIME":
                return new LocalDateTime[] {};
            case "POINT":
                return new PointValue[] {};
            case "TIME":
                return new OffsetTime[] {};
            case "DATE":
                return new LocalDate[] {};
            case "DURATION":
                return new DurationValue[] {};
        }
        throw new IllegalStateException("Type " + type + " not supported.");
    }

}
