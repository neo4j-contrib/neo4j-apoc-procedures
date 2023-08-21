package apoc.export.parquet;

import apoc.Pools;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.logging.Log;
import org.neo4j.procedure.TerminationGuard;

import java.util.Iterator;
import java.util.Map;


public class ExportParquetResultStreamStrategy extends ExportParquetStreamStrategy<Map<String,Object>, Result> {
    public ExportParquetResultStreamStrategy(GraphDatabaseService db, Pools pools, TerminationGuard terminationGuard, Log logger, ParquetExportType exportType) {
        super(db, pools, terminationGuard, logger, exportType);
    }

    @Override
    public Iterator<Map<String,Object>> toIterator(Result data) {
        return data.stream()
                .iterator();
    }
}
