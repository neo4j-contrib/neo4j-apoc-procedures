package apoc.export.parquet;

import apoc.util.CompressionAlgo;
import apoc.util.ExtendedUtil;
import apoc.util.FileUtils;
import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.neo4j.graphdb.security.URLAccessChecker;
import org.neo4j.graphdb.security.URLAccessValidationError;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.concurrent.TimeUnit;


public class ParquetReadUtil {

    public static Object toValidValue(Object object, String field, ParquetConfig config) {
        Map<String, Object> mapping = config.getMapping();
        return ExtendedUtil.toValidValue(object, field, mapping);
    }

    public static java.util.concurrent.TimeUnit toTimeUnitJava(LogicalTypeAnnotation.TimeUnit unit) {
        return switch (unit) {
            case NANOS -> TimeUnit.NANOSECONDS;
            case MICROS -> TimeUnit.MICROSECONDS;
            case MILLIS -> TimeUnit.MILLISECONDS;
        };
    }

    public static InputFile getInputFile(Object source, URLAccessChecker urlAccessChecker) throws IOException, URISyntaxException, URLAccessValidationError {
        return new ParquetStream(FileUtils.inputStreamFor(source, null, null, CompressionAlgo.NONE.name(), urlAccessChecker).readAllBytes());
    }

    public static ApocParquetReader getReader(Object source, ParquetConfig conf, URLAccessChecker urlAccessChecker) {
        try {
            return new ApocParquetReader(getInputFile(source, urlAccessChecker), conf);
        } catch (Exception e) {
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