package apoc.export.parquet;

import apoc.PoolsExtended;
import apoc.export.util.ProgressReporterExtended;
import apoc.result.ExportProgressInfoExtended;
import apoc.util.FileUtilsExtended;
import apoc.util.QueueBasedSpliteratorExtended;
import apoc.util.QueueUtilExtended;
import apoc.util.UtilExtended;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.schema.MessageType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.logging.Log;
import org.neo4j.procedure.TerminationGuard;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;


public abstract class ExportParquetFileStrategy<TYPE, IN> implements ExportParquetStrategy<IN, Stream<ExportProgressInfoExtended>> {

    private String fileName;
    private final GraphDatabaseService db;
    private final PoolsExtended pools;
    private final TerminationGuard terminationGuard;
    private final Log logger;
    private final ParquetExportType exportType;
    ParquetWriter writer;

    public ExportParquetFileStrategy(String fileName, GraphDatabaseService db, PoolsExtended pools, TerminationGuard terminationGuard, Log logger, ParquetExportType exportType) {
        this.fileName = fileName;
        this.db = db;
        this.pools = pools;
        this.terminationGuard = terminationGuard;
        this.logger = logger;
        this.exportType = exportType;
    }

    public Stream<ExportProgressInfoExtended> export(IN data, ParquetConfig config) {

        ExportProgressInfoExtended progressInfo = new ExportProgressInfoExtended(fileName, getSource(data), "parquet");
        progressInfo.setBatches(config.getBatchSize());
        ProgressReporterExtended reporter = new ProgressReporterExtended(null, null, progressInfo);

        final BlockingQueue<ExportProgressInfoExtended> queue = new ArrayBlockingQueue<>(10);
        UtilExtended.inTxFuture(pools.getDefaultExecutorService(), db, tx -> {
            int batchCount = 0;
            List<TYPE> rows = new ArrayList<>(config.getBatchSize());
            ParquetBufferedWriter parquetBufferedWriter = new ParquetBufferedWriter(FileUtilsExtended.getOutputStream(fileName));
            ExampleParquetWriter.Builder builder = ExampleParquetWriter.builder(parquetBufferedWriter);

            try {
                Iterator<TYPE> it = toIterator(reporter, data);
                while (!UtilExtended.transactionIsTerminated(terminationGuard) && it.hasNext()) {
                    rows.add(it.next());

                    if (batchCount > 0 && batchCount % config.getBatchSize() == 0) {
                        writeBatch(builder, rows, data, config);
                    }
                    ++batchCount;
                }
                if (!rows.isEmpty()) {
                    writeBatch(builder, rows, data, config);
                }
                QueueUtilExtended.put(queue, progressInfo, 10);
                return true;
            } catch (Exception e) {
                logger.error("Exception while extracting Parquet data:", e);
            } finally {
                closeWriter();
                reporter.done();
                QueueUtilExtended.put(queue, ExportProgressInfoExtended.EMPTY, 10);
            }
            return true;
        });

        QueueBasedSpliteratorExtended<ExportProgressInfoExtended> spliterator = new QueueBasedSpliteratorExtended<>(queue, ExportProgressInfoExtended.EMPTY, terminationGuard, Integer.MAX_VALUE);
        return StreamSupport.stream(spliterator, false);
    }

    private void closeWriter() {
        if (this.writer == null) return;
        try {
            this.writer.close();
        } catch (IOException ignored) {}
    }

    private void writeBatch(ExampleParquetWriter.Builder builder, List<TYPE> rows, IN data, ParquetConfig config) {

        List conf = exportType.createConfig(rows, data, config);
        MessageType schema = exportType.schemaFor(db, conf);

        if (writer == null) {
            this.writer = getBuild(schema, builder);
        }
        writeRows(rows, writer, exportType, schema);
    }

    public abstract String getSource(IN data);

    public abstract Iterator<TYPE> toIterator(ProgressReporterExtended reporter, IN data);
}
