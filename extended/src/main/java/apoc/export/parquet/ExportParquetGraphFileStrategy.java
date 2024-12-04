package apoc.export.parquet;

import apoc.PoolsExtended;
import apoc.cypher.export.SubGraphExtended;
import apoc.export.util.ProgressReporterExtended;
import apoc.result.ExportProgressInfoExtended;
import apoc.util.collection.IterablesExtended;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.logging.Log;
import org.neo4j.procedure.TerminationGuard;

import java.util.Iterator;
import java.util.stream.Stream;

public class ExportParquetGraphFileStrategy extends ExportParquetFileStrategy<Entity, SubGraphExtended>  {
    public ExportParquetGraphFileStrategy(String fileName, GraphDatabaseService db, PoolsExtended pools, TerminationGuard terminationGuard, Log logger, ParquetExportType exportType) {
        super(fileName, db, pools, terminationGuard, logger, exportType);
    }

    @Override
    public Stream<ExportProgressInfoExtended> export(SubGraphExtended data, ParquetConfig config) {
        return super.export(data, config);
    }

    @Override
    public String getSource(SubGraphExtended subGraph) {
        return String.format("graph: nodes(%d), rels(%d)", IterablesExtended.count(subGraph.getNodes()), IterablesExtended.count(subGraph.getRelationships()));
    }

    @Override
    public Iterator<Entity> toIterator(ProgressReporterExtended reporter, SubGraphExtended data) {
        return Stream.concat(IterablesExtended.stream(data.getNodes()), IterablesExtended.stream(data.getRelationships()))
                .map(entity -> {
                    reporter.update(entity instanceof Node ? 1 : 0,
                            entity instanceof Relationship ? 1 : 0, 0);
                    return entity;
                })
                .iterator();
    }
}
