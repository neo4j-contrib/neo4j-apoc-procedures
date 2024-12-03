package apoc.export.parquet;

import apoc.PoolsExtended;
import apoc.cypher.export.SubGraphExtended;
import apoc.util.collection.IterablesExtended;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.logging.Log;
import org.neo4j.procedure.TerminationGuard;

import java.util.Iterator;
import java.util.stream.Stream;

public class ExportParquetGraphStreamStrategy extends ExportParquetStreamStrategy<Entity, SubGraphExtended> {
    public ExportParquetGraphStreamStrategy(GraphDatabaseService db, PoolsExtended pools, TerminationGuard terminationGuard, Log logger, ParquetExportType exportType) {
        super(db, pools, terminationGuard, logger, exportType);
    }

    @Override
    public Iterator<Entity> toIterator(SubGraphExtended data) {
        return Stream.concat(IterablesExtended.stream(data.getNodes()), IterablesExtended.stream(data.getRelationships()))
                .iterator();
    }
}
