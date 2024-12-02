package apoc.export.arrow;

import static apoc.export.arrow.ArrowUtils.FIELD_ID;
import static apoc.export.arrow.ArrowUtils.FIELD_LABELS;
import static apoc.export.arrow.ArrowUtils.FIELD_SOURCE_ID;
import static apoc.export.arrow.ArrowUtils.FIELD_TARGET_ID;
import static apoc.export.arrow.ArrowUtils.FIELD_TYPE;

import apoc.Extended;
import apoc.Pools;
import apoc.export.util.BatchTransaction;
import apoc.export.util.ProgressReporter;
import apoc.result.ProgressInfo;
import apoc.util.FileUtils;
import apoc.util.JsonUtil;
import apoc.util.Util;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.json.JsonWriteFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.values.storable.DateTimeValue;
import org.neo4j.values.storable.DateValue;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.LocalDateTimeValue;
import org.neo4j.values.storable.LocalTimeValue;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.TimeValue;
import org.neo4j.values.storable.Values;

@Extended
public class ImportArrow {

    @Context
    public Pools pools;

    @Context
    public GraphDatabaseService db;

    @Procedure(name = "apoc.import.arrow", mode = Mode.WRITE)
    @Description("Imports arrow from the provided arrow file or byte array")
    public Stream<ProgressInfo> importFile(
            @Name("input") Object input, @Name(value = "config", defaultValue = "{}") Map<String, Object> config)
            throws Exception {

        ProgressInfo result = Util.inThread(pools, () -> {
            String file = null;
            String sourceInfo = "binary";
            if (input instanceof String) {
                file = (String) input;
                sourceInfo = "file";
            }

            final ArrowConfig conf = new ArrowConfig(config);

            final Map<Long, Long> idMapping = new HashMap<>();

            AtomicInteger counter = new AtomicInteger();
            try (ArrowReader reader = getReader(input);
                    VectorSchemaRoot schemaRoot = reader.getVectorSchemaRoot()) {

                final ProgressReporter reporter =
                        new ProgressReporter(null, null, new ProgressInfo(file, sourceInfo, "arrow"));
                BatchTransaction btx = new BatchTransaction(db, conf.getBatchSize(), reporter);
                try {
                    while (hasElements(counter, reader, schemaRoot)) {

                        final Map<String, Object> row = schemaRoot.getFieldVectors().stream()
                                .collect(
                                        HashMap::new,
                                        (map, fieldVector) -> {
                                            Object read = read(fieldVector, counter.get(), conf);
                                            if (read == null) {
                                                return;
                                            }
                                            map.put(fieldVector.getName(), read);
                                        },
                                        HashMap::putAll);

                        String relType = (String) row.remove(FIELD_TYPE.getName());
                        if (relType == null) {
                            // is node
                            String[] stringLabels = (String[]) row.remove(FIELD_LABELS.getName());
                            Label[] labels = Optional.ofNullable(stringLabels)
                                    .map(l -> Arrays.stream(l).map(Label::label).toArray(Label[]::new))
                                    .orElse(new Label[] {});
                            final Node node = btx.getTransaction().createNode(labels);

                            long id = (long) row.remove(FIELD_ID.getName());
                            idMapping.put(id, node.getId());

                            addProps(row, node);
                            reporter.update(1, 0, row.size());
                        } else {
                            // is relationship
                            long sourceId = (long) row.remove(FIELD_SOURCE_ID.getName());
                            Long idSource = idMapping.get(sourceId);
                            final Node source = btx.getTransaction().getNodeById(idSource);

                            long targetId = (long) row.remove(FIELD_TARGET_ID.getName());
                            Long idTarget = idMapping.get(targetId);
                            final Node target = btx.getTransaction().getNodeById(idTarget);

                            final Relationship rel =
                                    source.createRelationshipTo(target, RelationshipType.withName(relType));
                            addProps(row, rel);
                            reporter.update(0, 1, row.size());
                        }

                        counter.incrementAndGet();
                        btx.increment();
                    }

                    btx.commit();
                } catch (RuntimeException e) {
                    btx.rollback();
                    throw e;
                } finally {
                    btx.close();
                }

                return reporter.getTotal();
            }
        });

        return Stream.of(result);
    }

    private ArrowReader getReader(Object input) throws IOException {
        RootAllocator allocator = new RootAllocator();
        if (input instanceof String) {
            final SeekableByteChannel channel =
                    FileUtils.inputStreamFor(input, null, null, null).asChannel();
            return new ArrowFileReader(channel, allocator);
        }
        ByteArrayInputStream inputStream = new ByteArrayInputStream((byte[]) input);
        return new ArrowStreamReader(inputStream, allocator);
    }

    private static boolean hasElements(AtomicInteger counter, ArrowReader reader, VectorSchemaRoot schemaRoot)
            throws IOException {
        if (counter.get() >= schemaRoot.getRowCount()) {
            if (reader.loadNextBatch()) {
                counter.set(0);
            } else {
                return false;
            }
        }

        return true;
    }

    private static Object read(FieldVector fieldVector, int index, ArrowConfig conf) {

        if (fieldVector.isNull(index)) {
            return null;
        } else if (fieldVector instanceof BitVector) {
            BitVector fe = (BitVector) fieldVector;
            return fe.get(index) == 1;
        } else {
            Object object = fieldVector.getObject(index);
            if (object instanceof Collection && ((Collection) object).isEmpty()) {
                return null;
            }
            return toValidValue(object, fieldVector.getName(), conf.getMapping());
        }
    }

    private void addProps(Map<String, Object> row, Entity rel) {
        row.forEach(rel::setProperty);
    }

    /**
     * Analog to ArrowConfig present in APOC Core.
     * TODO Merge these 2 classes when arrow procedure will be moved to APOC Extended
     */
    public static class ArrowConfig {

        private final int batchSize;
        private final Map<String, Object> mapping;

        public ArrowConfig(Map<String, Object> config) {
            if (config == null) {
                config = Collections.emptyMap();
            }
            this.mapping = (Map<String, Object>) config.getOrDefault("mapping", Map.of());
            this.batchSize = Util.toInteger(config.getOrDefault("batchSize", 2000));
        }

        public int getBatchSize() {
            return batchSize;
        }

        public Map<String, Object> getMapping() {
            return mapping;
        }
    }

    public static Object toValidValue(Object object, String field, Map<String, Object> mapping) {
        Object fieldName = mapping.get(field);
        if (object != null && fieldName != null) {
            return convertValue(object.toString(), fieldName.toString());
        }

        if (object instanceof Collection) {
            // if there isn't a mapping config, we convert the list to a String[]
            return ((Collection<?>) object)
                    .stream()
                            .map(i -> toValidValue(i, field, mapping))
                            .collect(Collectors.toList())
                            .toArray(new String[0]);
        }
        if (object instanceof Map) {
            return ((Map<String, Object>) object)
                    .entrySet().stream()
                            .collect(Collectors.toMap(
                                    Map.Entry::getKey, e -> toValidValue(e.getValue(), field, mapping)));
        }
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
            case "Node":
            case "Relationship":
                return JsonUtil.parse(value, null, Map.class);
            case "NO_VALUE":
                return null;
            default:
                // If ends with "Array", for example StringArray
                if (typeName.endsWith("Array")) {
                    value = StringUtils.removeStart(value, "[");
                    value = StringUtils.removeEnd(value, "]");
                    String array = typeName.replace("Array", "");

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
        } catch (RuntimeException e) {
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
        switch (type) {
            case "Long":
                return new Long[] {};
            case "Integer":
                return new Integer[] {};
            case "Double":
                return new Double[] {};
            case "Float":
                return new Float[] {};
            case "Boolean":
                return new Boolean[] {};
            case "Byte":
                return new Byte[] {};
            case "Short":
                return new Short[] {};
            case "Char":
                return new Character[] {};
            case "String":
                return new String[] {};
            case "DateTime":
                return new ZonedDateTime[] {};
            case "LocalTime":
                return new LocalTime[] {};
            case "LocalDateTime":
                return new LocalDateTime[] {};
            case "Point":
                return new PointValue[] {};
            case "Time":
                return new OffsetTime[] {};
            case "Date":
                return new LocalDate[] {};
            case "Duration":
                return new DurationValue[] {};
            default:
                throw new IllegalStateException("Type " + type + " not supported.");
        }
    }
}
