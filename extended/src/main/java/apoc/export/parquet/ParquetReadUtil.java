package apoc.export.parquet;

import apoc.ApocConfig;
import apoc.util.JsonUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.neo4j.values.storable.DateTimeValue;
import org.neo4j.values.storable.DateValue;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.LocalDateTimeValue;
import org.neo4j.values.storable.LocalTimeValue;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.TimeValue;
import org.neo4j.values.storable.Values;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static apoc.util.FileUtils.changeFileUrlIfImportDirectoryConstrained;

public class ParquetReadUtil {

    public static Object toValidValue(Object object, String field, ParquetConfig config) {
        Object fieldName = config.getMapping().get(field);
        if (object != null && fieldName != null) {
            return convertValue(object.toString(), fieldName.toString());
        }

        if (object instanceof Collection) {
            // if there isn't a mapping config, we convert the list to a String[]
            return ((Collection<?>) object).stream()
                    .map(i -> toValidValue(i, field, config))
                    .collect(Collectors.toList())
                    .toArray(new String[0]);
        }
        if (object instanceof Map) {
            return ((Map<String, Object>) object).entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, e -> toValidValue(e.getValue(), field, config)));
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
                return PointValue.parse(value);
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

                    final Object[] prototype = getPrototypeFor(array);
                    return Arrays.stream(value.split(","))
                            .map(item -> convertValue(StringUtils.trim(item), array))
                            .toList()
                            .toArray(prototype);
                }
                return value;
        }
    }

    // similar to CsvPropertyConverter
    static Object[] getPrototypeFor(String type) {
        switch (type) {
            case "Long":
                return new Long[]{};
            case "Integer":
                return new Integer[]{};
            case "Double":
                return new Double[]{};
            case "Float":
                return new Float[]{};
            case "Boolean":
                return new Boolean[]{};
            case "Byte":
                return new Byte[]{};
            case "Short":
                return new Short[]{};
            case "Char":
                return new Character[]{};
            case "String":
                return new String[]{};
            case "DateTime":
                return new ZonedDateTime[]{};
            case "LocalTime":
                return new LocalTime[]{};
            case "LocalDateTime":
                return new LocalDateTime[]{};
            case "Point":
                return new PointValue[]{};
            case "Time":
                return new OffsetTime[]{};
            case "Date":
                return new LocalDate[]{};
            case "Duration":
                return new DurationValue[]{};
            default:
                throw new IllegalStateException("Type " + type + " not supported.");
        }
    }

    public static java.util.concurrent.TimeUnit toTimeUnitJava(LogicalTypeAnnotation.TimeUnit unit) {
        return switch (unit) {
            case NANOS -> TimeUnit.NANOSECONDS;
            case MICROS -> TimeUnit.MICROSECONDS;
            case MILLIS -> TimeUnit.MILLISECONDS;
        };
    }


    public static InputFile getInputFile(Object source) throws IOException {
        if (source instanceof String) {
            ApocConfig.apocConfig().isImportFileEnabled();
            String fileName = changeFileUrlIfImportDirectoryConstrained((String) source);
            Path file = new Path(fileName);
            return HadoopInputFile.fromPath(file, new Configuration());
        }
        return new ParquetStream((byte[]) source);
    }

    public static ApocParquetReader getReader(Object source, ParquetConfig conf) {

        try {
            return new ApocParquetReader(getInputFile(source), conf);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static class ParquetStream implements InputFile {
        private final byte[] data;

        private static class SeekableByteArrayInputStream extends ByteArrayInputStream {
            public SeekableByteArrayInputStream(byte[] buf) {
                super(buf);
            }

            public void setPos(int pos) {
                this.pos = pos;
            }

            public int getPos() {
                return this.pos;
            }
        }

        public ParquetStream(byte[] stream) {
            this.data = stream;
        }

        @Override
        public long getLength() {
            return this.data.length;
        }

        @Override
        public SeekableInputStream newStream() {
            return new DelegatingSeekableInputStream(new SeekableByteArrayInputStream(this.data)) {
                @Override
                public void seek(long newPos) {
                    ((SeekableByteArrayInputStream) this.getStream()).setPos((int) newPos);
                }

                @Override
                public long getPos() {
                    return ((SeekableByteArrayInputStream) this.getStream()).getPos();
                }
            };
        }
    }
}
