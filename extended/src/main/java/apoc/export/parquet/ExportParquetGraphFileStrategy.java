package apoc.export.parquet;

import apoc.Pools;
import apoc.export.util.ProgressReporter;
import apoc.result.ProgressInfo;
import apoc.util.collection.Iterables;
import org.neo4j.cypher.export.SubGraph;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.logging.Log;
import org.neo4j.procedure.TerminationGuard;

import java.util.Iterator;
import java.util.stream.Stream;

public class ExportParquetGraphFileStrategy extends ExportParquetFileStrategy<Entity, SubGraph>  {
    public ExportParquetGraphFileStrategy(String fileName, GraphDatabaseService db, Pools pools, TerminationGuard terminationGuard, Log logger, ParquetExportType exportType) {
        super(fileName, db, pools, terminationGuard, logger, exportType);
    }

    @Override
    public Stream<ProgressInfo> export(SubGraph data, ParquetConfig config) {
        return super.export(data, config);
    }

    @Override
    public String getSource(SubGraph subGraph) {
        return String.format("graph: nodes(%d), rels(%d)", Iterables.count(subGraph.getNodes()), Iterables.count(subGraph.getRelationships()));
    }

    @Override
    public Iterator<Entity> toIterator(ProgressReporter reporter, SubGraph data) {
        return Stream.concat(Iterables.stream(data.getNodes()), Iterables.stream(data.getRelationships()))
                .map(entity -> {
                    reporter.update(entity instanceof Node ? 1 : 0,
                            entity instanceof Relationship ? 1 : 0, 0);
                    return entity;
                })
                .iterator();
    }
}
