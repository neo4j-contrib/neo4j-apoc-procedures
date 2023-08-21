package apoc.export.parquet;

import apoc.Pools;
import apoc.export.util.ProgressReporter;
import apoc.result.ProgressInfo;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Result;
import org.neo4j.logging.Log;
import org.neo4j.procedure.TerminationGuard;

import java.util.Iterator;
import java.util.Map;
import java.util.stream.Stream;


public class ExportParquetResultFileStrategy extends ExportParquetFileStrategy<Map<String,Object>, Result> {
    public ExportParquetResultFileStrategy(String fileName, GraphDatabaseService db, Pools pools, TerminationGuard terminationGuard, Log logger, ParquetExportType exportType) {
        super(fileName, db, pools, terminationGuard, logger, exportType);
    }

    @Override
    public String getSource(Result result) {
        return String.format("statement: cols(%d)", result.columns().size());
    }

    @Override
    public Iterator<Map<String, Object>> toIterator(ProgressReporter reporter, Result data) {

        return data.stream()
                .peek(row -> {
                    row.forEach((key, val) -> {
                        final boolean notNodeNorRelationship = !(val instanceof Node) && !(val instanceof Relationship);
                        reporter.update(val instanceof Node ? 1 : 0,
                                val instanceof Relationship ? 1 : 0,
                                notNodeNorRelationship ? 1 : 0);
                        if (notNodeNorRelationship) {
                            reporter.nextRow();
                        }
                    });
                })
                .iterator();
    }

    @Override
    public Stream<ProgressInfo> export(Result data, ParquetConfig config) {
        return super.export(data, config);
    }


}
