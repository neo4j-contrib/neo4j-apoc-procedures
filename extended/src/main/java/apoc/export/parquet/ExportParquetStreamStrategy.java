package apoc.export.parquet;

import apoc.Pools;
import apoc.result.ByteArrayResult;
import apoc.util.QueueBasedSpliterator;
import apoc.util.QueueUtil;
import apoc.util.Util;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.schema.MessageType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.logging.Log;
import org.neo4j.procedure.TerminationGuard;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public abstract class ExportParquetStreamStrategy<TYPE, IN> implements ExportParquetStrategy<IN, Stream<ByteArrayResult>>  {

    private final GraphDatabaseService db;
    private final Pools pools;
    private final TerminationGuard terminationGuard;
    private final Log logger;
    private final ParquetExportType exportType;

    public ExportParquetStreamStrategy(GraphDatabaseService db, Pools pools, TerminationGuard terminationGuard, Log logger, ParquetExportType exportType) {
        this.db = db;
        this.pools = pools;
        this.terminationGuard = terminationGuard;
        this.logger = logger;
        this.exportType = exportType;
    }

    public Stream<ByteArrayResult> export(IN data, ParquetConfig config) {
        final BlockingQueue<ByteArrayResult> queue = new ArrayBlockingQueue<>(100);

        Util.inTxFuture(pools.getDefaultExecutorService(), db, tx -> {
            int batchCount = 0;
            List<TYPE> rows = new ArrayList<>(config.getBatchSize());

            try {
                Iterator<TYPE> it = toIterator(data);
                while (!Util.transactionIsTerminated(terminationGuard) && it.hasNext()) {
                    rows.add(it.next());

                    if (batchCount > 0 && batchCount % config.getBatchSize() == 0) {
                        byte[] bytes = writeBatch(rows, data, config);
                        QueueUtil.put(queue, new ByteArrayResult(bytes), 10);
                    }
                    ++batchCount;
                }
                if (!rows.isEmpty()) {
                    byte[] bytes = writeBatch(rows, data, config);
                    QueueUtil.put(queue, new ByteArrayResult(bytes), 10);
                }
                return true;
            } catch (Exception e) {
                logger.error("Exception while extracting Parquet data:", e);
            } finally {
                QueueUtil.put(queue, ByteArrayResult.NULL, 10);
            }
            return true;
        });

        QueueBasedSpliterator<ByteArrayResult> spliterator = new QueueBasedSpliterator<>(queue, ByteArrayResult.NULL, terminationGuard, Integer.MAX_VALUE);
        return StreamSupport.stream(spliterator, false);
    }

    private byte[] writeBatch(List<TYPE> rows, IN data, ParquetConfig config) {
        List conf = exportType.createConfig(rows, data, config);
        MessageType schema = exportType.schemaFor(db, conf);
        try (ByteArrayOutputStream bytesOut = new ByteArrayOutputStream()) {
            ParquetBufferedWriter out = new ParquetBufferedWriter(bytesOut);

            try (ParquetWriter<Group> writer = getBuild(schema, ExampleParquetWriter.builder(out))) {
                writeRows(rows, writer, exportType, schema);
            }

            return bytesOut.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public abstract Iterator<TYPE> toIterator(IN data);

}
