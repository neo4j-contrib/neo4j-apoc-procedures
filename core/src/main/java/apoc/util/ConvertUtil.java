package apoc.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.json.JsonWriteFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.time.Duration;
import java.util.Collection;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.exceptions.Neo4jException;
import org.neo4j.values.storable.DateTimeValue;
import org.neo4j.values.storable.DateValue;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.LocalDateTimeValue;
import org.neo4j.values.storable.LocalTimeValue;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.TimeValue;
import org.neo4j.values.storable.Values;

public class ConvertUtil {
    public static Object toValidValue(Object object, String field, Map<String, Object> mapping) {
        Object fieldName = mapping.get(field);
        if (object != null && fieldName != null) {
            return convertValue(object.toString(), fieldName.toString());
        }

        if (object instanceof Collection) {
            // if there isn't a mapping config, we convert the list to a String[]
            List<Object> list = ((Collection<?>) object)
                    .stream().map(i -> toValidValue(i, field, mapping)).collect(Collectors.toList());

            try {
                return list.toArray(new String[0]);
            } catch (ArrayStoreException e) {
                return list.toArray(new Object[0]);
            }
        }
        if (object instanceof Map) {
            return ((Map<String, Object>) object)
                    .entrySet().stream()
                            .collect(Collectors.toMap(
                                    Map.Entry::getKey, e -> toValidValue(e.getValue(), field, mapping)));
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
        typeName = typeName.toLowerCase(); // Suitable to work with Parquet/Arrow/Gexf
        switch (typeName) {
                // {"crs":"wgs-84-3d","latitude":13.1,"longitude":33.46789,"height":100.0}
            case "point":
                return getPointValue(value);
            case "localdatetime":
                return LocalDateTimeValue.parse(value).asObjectCopy();
            case "localtime":
                return LocalTimeValue.parse(value).asObjectCopy();
            case "datetime":
                return DateTimeValue.parse(value, () -> ZoneId.of("Z")).asObjectCopy();
            case "time":
                return TimeValue.parse(value, () -> ZoneId.of("Z")).asObjectCopy();
            case "date":
                return DateValue.parse(value).asObjectCopy();
            case "duration":
                return DurationValue.parse(value);
            case "boolean":
                return Boolean.parseBoolean(value);
            case "char":
                return value.charAt(0);
            case "byte":
                return value.getBytes();
            case "double":
                return Double.parseDouble(value);
            case "float":
                return Float.parseFloat(value);
            case "short":
                return Short.parseShort(value);
            case "int":
            case "integer":
                return Integer.parseInt(value);
            case "long":
                return Long.parseLong(value);
            case "node":
            case "relationship":
                return JsonUtil.parse(value, null, Map.class);
            case "no_value":
            case "NO_VALUE":
                return null;
            case "listboolean":
                value = StringUtils.removeStart(value, "[");
                value = StringUtils.removeEnd(value, "]");
                String dataType = typeName.replace("array", "").replace("list", "");

                final Object[] arr = getPrototypeFor(dataType);
                return Arrays.stream(value.split(","))
                        .map(item -> convertValue(StringUtils.trim(item), dataType))
                        .collect(Collectors.toList())
                        .toArray(arr);
            default:
                // If ends with "Array", for example StringArray
                if (typeName.endsWith("array") || typeName.startsWith("list")) {
                    value = StringUtils.removeStart(value, "[");
                    value = StringUtils.removeEnd(value, "]");
                    String array = typeName.replace("array", "").replace("list", "");

                    final Object[] prototype = getPrototypeFor(array);
                    return Arrays.stream(value.split(","))
                            .map(item -> convertValue(StringUtils.trim(item), array))
                            .collect(Collectors.toList())
                            .toArray(prototype);
                }
                return value;
        }
    }

    private static PointValue getPointValue(String value) {
        try {
            return PointValue.parse(value);
        } catch (Neo4jException e) {
            // fallback in case of double-quotes, e.g.
            // {"crs":"wgs-84-3d","latitude":13.1,"longitude":33.46789,"height":100.0}
            // we remove the double quotes before parsing the result, e.g.
            // {crs:"wgs-84-3d",latitude:13.1,longitude:33.46789,height:100.0}
            ObjectMapper objectMapper = new ObjectMapper().disable(JsonWriteFeature.QUOTE_FIELD_NAMES.mappedFeature());
            try {
                Map readValue = objectMapper.readValue(value, Map.class);
                String stringWithoutKeyQuotes = objectMapper.writeValueAsString(readValue);
                return PointValue.parse(stringWithoutKeyQuotes);
            } catch (JsonProcessingException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    // similar to CsvPropertyConverter
    public static Object[] getPrototypeFor(String type) {
        type = type.toLowerCase(); // Suitable to work with Parquet/Arrow/Gexf
        switch (type) {
            case "long":
                return new Long[] {};
            case "integer":
                return new Integer[] {};
            case "double":
                return new Double[] {};
            case "float":
                return new Float[] {};
            case "boolean":
                return new Boolean[] {};
            case "byte":
                return new Byte[] {};
            case "short":
                return new Short[] {};
            case "char":
                return new Character[] {};
            case "string":
                return new String[] {};
            case "datetime":
                return new ZonedDateTime[] {};
            case "localtime":
                return new LocalTime[] {};
            case "localdatetime":
                return new LocalDateTime[] {};
            case "point":
                return new PointValue[] {};
            case "time":
                return new OffsetTime[] {};
            case "date":
                return new LocalDate[] {};
            case "duration":
                return new DurationValue[] {};
            default:
                throw new IllegalStateException("Type " + type + " not supported.");
        }
    }

    public static <T> T withBackOffRetries(
            Supplier<T> func,
            boolean retry,
            int backoffRetry,
            boolean exponential,
            Consumer<Exception> exceptionHandler) {
        T result;
        backoffRetry = backoffRetry < 1 ? 5 : backoffRetry;
        int countDown = backoffRetry;
        exceptionHandler = Objects.requireNonNullElse(exceptionHandler, exe -> {});
        while (true) {
            try {
                result = func.get();
                break;
            } catch (Exception e) {
                if (!retry || countDown < 1) throw e;
                exceptionHandler.accept(e);
                countDown--;
                long delay = getDelay(backoffRetry, countDown, exponential);
                backoffSleep(delay);
            }
        }
        return result;
    }

    private static void backoffSleep(long millis) {
        sleep(millis, "Operation interrupted during backoff");
    }

    public static void sleep(long millis, String interruptedMessage) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(interruptedMessage, ie);
        }
    }

    private static long getDelay(int backoffRetry, int countDown, boolean exponential) {
        int backOffTime = backoffRetry - countDown;
        long sleepMultiplier = exponential
                ? (long) Math.pow(2, backOffTime)
                : // Exponential retry progression
                backOffTime; // Linear retry progression
        return Math.min(
                Duration.ofSeconds(1).multipliedBy(sleepMultiplier).toMillis(),
                Duration.ofSeconds(30).toMillis() // Max 30s
        );
    }

    public static String joinStringLabels(Collection<String> labels) {
        return CollectionUtils.isNotEmpty(labels)
                ? ":" + labels.stream().map(Util::quote).collect(Collectors.joining(":"))
                : "";
    }
}
