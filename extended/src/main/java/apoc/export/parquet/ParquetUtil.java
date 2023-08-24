package apoc.export.parquet;


import apoc.convert.ConvertUtils;
import apoc.util.JsonUtil;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupFactory;
import org.apache.parquet.example.data.simple.NanoTime;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.Map;
import java.util.Set;

import static apoc.util.Util.labelStrings;
import static org.apache.parquet.schema.LogicalTypeAnnotation.DateLogicalTypeAnnotation;
import static org.apache.parquet.schema.LogicalTypeAnnotation.ListLogicalTypeAnnotation;
import static org.apache.parquet.schema.LogicalTypeAnnotation.TimestampLogicalTypeAnnotation;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BOOLEAN;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.Types.GroupBuilder;
import static org.apache.parquet.schema.Types.optional;
import static org.apache.parquet.schema.Types.optionalList;

public class ParquetUtil {
    public static String FIELD_ID = "__id";
    public static String FIELD_LABELS = "__labels";
    public static String FIELD_SOURCE_ID = "__source_id";
    public static String FIELD_TARGET_ID = "__target_id";
    public static String FIELD_TYPE = "__type";

    public static String fromMetaType(apoc.meta.Types type) {
        switch (type) {
            case INTEGER:
                return "LONG";
            case FLOAT:
                return "DOUBLE";
            case LIST:
                String inner = type.toString().substring("LIST OF ".length()).trim();
                final apoc.meta.Types innerType = apoc.meta.Types.from(inner);
                if (innerType == apoc.meta.Types.LIST || innerType == apoc.meta.Types.MAP ) {
                    return "ANYARRAY";
                }
                return fromMetaType(innerType) + "ARRAY";
            default:
                return type.name().replaceAll("_", "").toUpperCase();
        }
    }

    public static Group mapToRecord(MessageType schema, Map<String, Object> map) {
        GroupFactory factory = new SimpleGroupFactory(schema);
        Group group = factory.newGroup();

        map.forEach((k, v)-> {
            try {
                Type type = schema.getType(k);
                if (type.getLogicalTypeAnnotation() instanceof ListLogicalTypeAnnotation) {
                    appendList(group, k, v);
                } else {
                    appendElement(group, k, v, schema);
                }
            } catch (Exception e2) {
                throw new RuntimeException(e2);
            }
        });
        return group;
    }

    public static void appendList(Group group, String k, Object v) {
        Group group1 = group.addGroup(k);
        ConvertUtils.convertToList(v).forEach(item -> {
            Group group2 = group1.addGroup(0);
            group2.add(0, item.toString());
        });
    }

    private static long writeDateMilliVector(Object value) {
        if (value instanceof Date) {
            return ((Date) value).getTime();
        } else if (value instanceof LocalDateTime) {
            return ((LocalDateTime) value)
                    .toInstant(ZoneOffset.UTC)
                    .toEpochMilli();
        } else if (value instanceof ZonedDateTime) {
            return ((ZonedDateTime) value)
                    .toInstant()
                    .toEpochMilli();
        } else if (value instanceof OffsetDateTime) {
            return ((OffsetDateTime) value)
                    .toInstant()
                    .toEpochMilli();
        } else {
            return (long) value;
        }
    }

    public static <T> void appendElement(Group group, String fieldName, Object value, MessageType schema) {
        if (value == null) {
            return;
        }

        PrimitiveType.PrimitiveTypeName typeName = schema.getType(fieldName)
                .asPrimitiveType()
                .getPrimitiveTypeName();
        if (typeName.equals(INT64)) {
            group.append(fieldName, writeDateMilliVector(value));
        } else if (typeName.equals(BINARY)) {
            group.append(fieldName, serializeValue(value));
        } else if (value instanceof Integer) {
            group.append(fieldName, (int) value);
        } else if (value instanceof Float) {
            group.append(fieldName, (float) value);
        } else if (value instanceof Double) {
            group.append(fieldName, (double) value);
        } else if (value instanceof Long) {
            group.append(fieldName, (long) value);
        } else if (value instanceof NanoTime) {
            group.append(fieldName, (NanoTime) value);
        } else if (value instanceof Boolean) {
            group.append(fieldName, (boolean) value);
        } else {
            // fallback
            group.append(fieldName, serializeValue(value));
        }

    }

    private static String serializeValue(Object val){
        if (val instanceof Node) {
            Node value = (Node) val;
            Map<String, Object> allProperties = value.getAllProperties();
            allProperties.put(FIELD_ID, value.getId());
            allProperties.put(FIELD_LABELS, labelStrings(value));
            return JsonUtil.writeValueAsString(allProperties);
        }
        if (val instanceof Relationship) {
            Relationship value = (Relationship) val;
            Map<String, Object> allProperties = value.getAllProperties();
            allProperties.put(FIELD_ID, value.getId());
            allProperties.put(FIELD_SOURCE_ID, value.getStartNodeId());
            allProperties.put(FIELD_TARGET_ID, value.getEndNodeId());
            allProperties.put(FIELD_TYPE, value.getType().name());
            return JsonUtil.writeValueAsString(allProperties);
        }
        if (val instanceof Map) {
            return JsonUtil.writeValueAsString(val);
        }
        return val.toString();
    }

    public static void addListItem(String fieldName, GroupBuilder test) {
        PrimitiveType element = optional(BINARY).named("element");
        GroupType groupType = optionalList()
                .element(element)
                .named(fieldName);
        test.addField(groupType);
    }

    static void toField(String fieldName, Set<String> propertyTypes, GroupBuilder builder) {

        if (propertyTypes.size() > 1) {
            // multi type handled as a string
            getSchemaFieldAssembler(builder, fieldName, "String");
        } else {
            getSchemaFieldAssembler(builder, fieldName, propertyTypes.iterator().next());
        }
    }

    public static void getField(GroupBuilder builder, PrimitiveType.PrimitiveTypeName type, String fieldName) {
        builder.addField(optional(type).named(fieldName));
    }

    private static void getSchemaFieldAssembler(GroupBuilder builder, String fieldName, String propertyType) {
        propertyType = propertyType.toUpperCase();

        switch (propertyType) {
            case "BOOLEAN" -> builder.addField(optional(BOOLEAN).named(fieldName));
            case "LONG" -> builder.addField(optional(INT64).named(fieldName));
            case "DOUBLE" -> builder.addField(optional(DOUBLE).named(fieldName));
            case "DATETIME" -> addDateTimeField(builder, fieldName, true);
            case "LOCALDATETIME" -> addDateTimeField(builder, fieldName, false);
            case "DATE" -> {
                PrimitiveType type = optional(INT64)
                        .as(DateLogicalTypeAnnotation.dateType())
                        .named(fieldName);
                builder.addField(type);
            }
            case "DURATION", "NODE", "RELATIONSHIP", "POINT" -> {
                // convert each type not manageable from parquet to string,
                // which can be re-imported via mapping config
                builder.addField(optional(BINARY).named(fieldName));
            }
            default -> {
                if (propertyType.endsWith("ARRAY")) {
                    // convert each type not manageable from parquet to string,
                    // which can be re-imported via mapping config
                    addListItem(fieldName, builder);
                } else {
                    builder.addField(optional(BINARY).named(fieldName));
                }
            }
        }
    }

    private static Types.BaseGroupBuilder addDateTimeField(GroupBuilder builder, String fieldName, boolean isAdjustedToUTC) {
        TimestampLogicalTypeAnnotation type = TimestampLogicalTypeAnnotation.timestampType(isAdjustedToUTC, LogicalTypeAnnotation.TimeUnit.MILLIS);
        PrimitiveType primitiveType = optional(INT64)
                .as(type)
                .named(fieldName);
        return builder.addField(primitiveType);
    }
}
