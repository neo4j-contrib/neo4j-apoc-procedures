package apoc.load;

import apoc.Extended;
import apoc.export.parquet.ApocParquetReader;
import apoc.export.parquet.ParquetConfig;
import apoc.result.MapResult;
import apoc.util.Util;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.io.IOException;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static apoc.export.parquet.ParquetReadUtil.getReader;

@Extended
public class LoadParquet {

    @Context public Log log;


    private static class ParquetSpliterator extends Spliterators.AbstractSpliterator<MapResult> {

        private final ApocParquetReader reader;

        public ParquetSpliterator(ApocParquetReader reader) {
            super(Long.MAX_VALUE, Spliterator.ORDERED);
            this.reader = reader;
        }

        @Override
        public synchronized boolean tryAdvance(Consumer<? super MapResult> action) {
            try {
                Map<String, Object> read = reader.getRecord();
                if (read != null) {
                    MapResult result = new MapResult(read);
                    action.accept(result);
                    return true;
                }

                return false;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Procedure(name = "apoc.load.parquet")
    @Description("Load parquet from the provided file or binary")
    public Stream<MapResult> load(
            @Name("input") Object input,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws IOException {

        ParquetConfig conf = new ParquetConfig(config);

        ApocParquetReader reader = getReader(input, conf);
        return StreamSupport.stream(new ParquetSpliterator(reader),false)
                .onClose(() -> Util.close(reader));
    }



}
