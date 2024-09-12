package apoc.convert;

import apoc.util.JsonUtil;
import org.apache.commons.lang3.StringUtils;

import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.TimeValue;
import org.neo4j.values.storable.Values;
import static apoc.export.csv.CsvPropertyConverter.getPrototypeFor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.json.JsonWriteFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import apoc.export.util.DurationValueSerializer;
import apoc.export.util.PointSerializer;
import apoc.export.util.TemporalSerializer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import org.neo4j.graphdb.spatial.Point;
import org.neo4j.values.storable.DurationValue;
import java.time.temporal.Temporal;
import java.util.List;
import java.util.Map;

public class ConvertExtendedUtil {
    public static Object parse(String value, Map<String, Object> config) throws JsonProcessingException {
        YAMLFactory factory = new YAMLFactory();

        List<String> enable = (List<String>) config.getOrDefault("enable", List.of());
        List<String> disable = (List<String>) config.getOrDefault("disable", List.of());
        enable.forEach(name -> factory.enable(YAMLGenerator.Feature.valueOf(name)));
        disable.forEach(name -> factory.disable(YAMLGenerator.Feature.valueOf(name)));

        ObjectMapper objectMapper = new ObjectMapper(factory);
        objectMapper.registerModule(YAML_MODULE);

        return objectMapper.readValue(value, Object.class);
    }

    public static <T> Object toValidYamlValue(Object object, String field, Map<String, Object> mapping, boolean start) {
        Object fieldName = field == null ? null : mapping.get(field);

        if (fieldName == null) {
            fieldName = start ? mapping.get("_") : null;
        }

        if (object instanceof Collection) {
            return ((Collection<?>) object).stream()
                    .map(i -> toValidYamlValue(i, field, mapping, start))
                    .collect(Collectors.toList())
                    .toArray(i -> new Object[] {});
        }
        if (object instanceof Map) {
            return ((Map<String, Object>) object).entrySet().stream()
                    .collect(
                            HashMap::new, // workaround for https://bugs.openjdk.java.net/browse/JDK-8148463
                            (mapAccumulator, entry) ->
                                    mapAccumulator.put(entry.getKey(), toValidYamlValue(entry.getValue(), entry.getKey(), mapping, false)),
                            HashMap::putAll);
        }

        if (object != null && fieldName != null) {
            return convertValue(object.toString(), fieldName.toString());
        }

        return getNeo4jValue(object);
    }

    private static Object getNeo4jValue(Object object) {
        try {
            // we test if is a valid Neo4j type
            Values.of(object);
            return object;
        } catch (Exception e) {
            // otherwise we try to coerce it
            return object.toString();
        }
    }

    /**
     * In case of complex type non-readable from Parquet, i.e. Duration, Point, List of Neo4j Types...
     * we can use the `mapping: {keyToConvert: valueTypeName}` config to convert them.
     * For example `mapping: {myPropertyKey: "DateArray"}`
     */
    private static Object convertValue(String value, String typeName) {
        switch (typeName) {
            // {"crs":"wgs-84-3d","latitude":13.1,"longitude":33.46789,"height":100.0}
            case "Point":
                return getPointValue(value);
            case "LocalDateTime":
                return LocalDateTimeValue.parse(value).asObjectCopy();
            case "LocalTime":
                return LocalTimeValue.parse(value).asObjectCopy();
            case "DateTime":
                return DateTimeValue.parse(value, () -> ZoneId.of("Z")).asObjectCopy();
            case "Time":
                return TimeValue.parse(value, () -> ZoneId.of("Z")).asObjectCopy();
            case "Date":
                return DateValue.parse(value).asObjectCopy();
            case "Duration":
                return DurationValue.parse(value);
            case "Char":
                return value.charAt(0);
            case "Byte":
                return value.getBytes();
            case "Double":
                return Double.parseDouble(value);
            case "Float":
                return Float.parseFloat(value);
            case "Short":
                return Short.parseShort(value);
            case "Int":
                return Integer.parseInt(value);
            case "Long":
                return Long.parseLong(value);
            case "Node", "Relationship":
                return JsonUtil.parse(value, null, Map.class);
            case "NO_VALUE":
                return null;
            default:
                // If ends with "Array", for example StringArray
                if (typeName.endsWith("Array")) {
                    value = StringUtils.removeStart(value, "[");
                    value = StringUtils.removeEnd(value, "]");
                    String array = typeName.replace("Array", "");
                    final Object[] prototype = getPrototypeFor( array.toUpperCase() );
                    return Arrays.stream(value.split(","))
                            .map(item -> convertValue(StringUtils.trim(item), array))
                            .collect(Collectors.toList())
                            .toArray(prototype);
                }
                return value;
        }
    }
}
