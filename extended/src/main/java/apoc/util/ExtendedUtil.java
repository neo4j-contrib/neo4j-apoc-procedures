package apoc.util;

import apoc.util.collection.Iterators;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.json.JsonWriteFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.WordUtils;
import org.neo4j.exceptions.Neo4jException;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.QueryExecutionType;
import org.neo4j.graphdb.Result;
import org.neo4j.procedure.Mode;
import org.neo4j.values.storable.DateTimeValue;
import org.neo4j.values.storable.DateValue;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.LocalDateTimeValue;
import org.neo4j.values.storable.LocalTimeValue;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.TimeValue;
import org.neo4j.values.storable.Values;

import java.math.BigInteger;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.TemporalAccessor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static apoc.export.cypher.formatter.CypherFormatterUtils.formatProperties;
import static apoc.export.cypher.formatter.CypherFormatterUtils.formatToString;
import static apoc.util.Util.getAllQueryProcs;

public class ExtendedUtil
{
    public static String dateFormat( TemporalAccessor value, String format){
        return Util.getFormat(format).format(value);
    }

    public static double doubleValue( Entity pc, String prop, Number defaultValue) {
        return Util.toDouble(pc.getProperty(prop, defaultValue));
    }

    public static Duration durationParse(String value) {
        return Duration.parse(value);
    }

    public static boolean isSumOutOfRange(long... numbers) {
        try {
            sumLongs(numbers).longValueExact();
            return false;
        } catch (ArithmeticException ae) {
            return true;
        }
    }

    private static BigInteger sumLongs(long... numbers) {
        return LongStream.of(numbers)
                .mapToObj(BigInteger::valueOf)
                .reduce(BigInteger.ZERO, (x, y) -> x.add(y));
    }

    public static String toCypherMap( Map<String, Object> map) {
        final StringBuilder builder = formatProperties(map);
        return "{" + formatToString(builder) + "}";
    }

    public static Object toValidValue(Object object, String field, Map<String, Object> mapping) {
        Object fieldName = mapping.get(field);
        if (object != null && fieldName != null) {
            return convertValue(object.toString(), fieldName.toString());
        }

        if (object instanceof Collection) {
            // if there isn't a mapping config, we convert the list to a String[]
            List<Object> list = ((Collection<?>) object).stream()
                    .map(i -> toValidValue(i, field, mapping))
                    .collect(Collectors.toList());

            try {
                return list.toArray(new String[0]);
            } catch (ArrayStoreException e) {
                return list.toArray(new Object[0]);
            }
        }
        if (object instanceof Map) {
            return ((Map<String, Object>) object).entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, e -> toValidValue(e.getValue(), field, mapping)));
        }
        return getNeo4jValue(object);
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
            case "node", "relationship":
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
                        .toList()
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
                            .toList()
                            .toArray(prototype);
                }
                return value;
        }
    }

    private static PointValue getPointValue(String value) {
        try {
            return PointValue.parse(value);
        } catch (Neo4jException e) {
            // fallback in case of double-quotes, e.g. {"crs":"wgs-84-3d","latitude":13.1,"longitude":33.46789,"height":100.0}
            // we remove the double quotes before parsing the result, e.g. {crs:"wgs-84-3d",latitude:13.1,longitude:33.46789,height:100.0}
            ObjectMapper objectMapper = new ObjectMapper()
                    .disable(JsonWriteFeature.QUOTE_FIELD_NAMES.mappedFeature());
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
        return switch (type) {
            case "long" -> new Long[]{};
            case "integer" -> new Integer[]{};
            case "double" -> new Double[]{};
            case "float" -> new Float[]{};
            case "boolean" -> new Boolean[]{};
            case "byte" -> new Byte[]{};
            case "short" -> new Short[]{};
            case "char" -> new Character[]{};
            case "string" -> new String[]{};
            case "datetime" -> new ZonedDateTime[]{};
            case "localtime" -> new LocalTime[]{};
            case "localdatetime" -> new LocalDateTime[]{};
            case "point" -> new PointValue[]{};
            case "time" -> new OffsetTime[]{};
            case "date" -> new LocalDate[]{};
            case "duration" -> new DurationValue[]{};
            default -> throw new IllegalStateException("Type " + type + " not supported.");
        };
    }

    // Similar to `boolean isQueryTypeValid` located in Util.java (APOC Core)
    public static QueryExecutionType.QueryType isQueryValid(Result result, QueryExecutionType.QueryType[] supportedQueryTypes) {
        QueryExecutionType.QueryType type = result.getQueryExecutionType().queryType();
        // if everything is ok return null, otherwise the current getQueryExecutionType().queryType()
        if (supportedQueryTypes != null && supportedQueryTypes.length != 0 && Stream.of(supportedQueryTypes).noneMatch(sqt -> sqt.equals(type))) {
            return type;
        }
        return null;
    }

    public static boolean procsAreValid(GraphDatabaseService db, Set<Mode> supportedModes, Result result) {
        if (supportedModes != null && !supportedModes.isEmpty()) {
            final ExecutionPlanDescription executionPlanDescription = result.getExecutionPlanDescription();
            // get procedures used in the query
            Set<String> queryProcNames = new HashSet<>();
            getAllQueryProcs(executionPlanDescription, queryProcNames);

            if (!queryProcNames.isEmpty()) {
                final Set<String> modes = supportedModes.stream().map(Mode::name).collect(Collectors.toSet());
                // check if sub-procedures have valid mode
                final Set<String> procNames = db.executeTransactionally("SHOW PROCEDURES YIELD name, mode where mode in $modes return name",
                        Map.of("modes", modes),
                        r -> Iterators.asSet(r.columnAs("name")));

                return procNames.containsAll(queryProcNames);
            }
        }

        return true;
    }

    public static void retryRunnable(long maxRetries, Runnable supplier) {
        retryRunnable(maxRetries, 0, supplier);
    }

    private static void retryRunnable(long maxRetries, long retry, Runnable consumer) {
        try {
            consumer.run();
        } catch (Exception e) {
            if (retry >= maxRetries) throw e;
            Util.sleep(100);
            retry++;
            retryRunnable(maxRetries, retry, consumer);
        }
    }

    public static void setProperties(Entity entity, Map<String, Object> props) {
        for (var entry: props.entrySet()) {
            entity.setProperty(entry.getKey(), entry.getValue());
        }
    }

    /**
     * Transform a list like: [ {key1: valueFoo1, key2: valueFoo2}, {key1: valueBar1, key2: valueBar2} ]
     * to a map like: { keyNew1: [valueFoo1, valueBar1], keyNew2: [valueFoo2, valueBar2] },
     * 
     * where mapKeys is e.g. {key1: keyNew1, key2: keyNew2}
     */
    public static Map<Object, List> listOfMapToMapOfLists(Map mapKeys, List<Map<String, Object>> vectors) {
        Map<Object, List> additionalBodies = new HashMap();
        for (var vector: vectors) {
            mapKeys.forEach((from, to) -> {
                mapEntryToList(additionalBodies, vector, from, to);
            });
        }
        return additionalBodies;
    }

    private static void mapEntryToList(Map<Object, List> map, Map<String, Object> vector, Object keyFrom, Object keyTo) {
        Object item = vector.get(keyFrom);
        if (item == null) {
            return;
        }
        
        map.compute(keyTo, (k, v) -> {
            if (v == null) {
                List<Object> list = new ArrayList<>();
                list.add(item);
                return list;
            }
            v.add(item);
            return v;
        });
    }
    
    public static float[] listOfNumbersToFloatArray(List<? extends Number> embedding) {
        float[] floats = new float[embedding.size()];
        int i = 0;
        for (var item: embedding) {
            floats[i] = item.floatValue();
            i++;
        }
        return floats;
    }
            
}
