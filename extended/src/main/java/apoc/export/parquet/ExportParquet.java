package apoc.export.parquet;

import apoc.ApocConfig;
import apoc.Description;
import apoc.Extended;
import apoc.Pools;
import apoc.export.util.NodesAndRelsSubGraph;
import apoc.result.ByteArrayResult;
import apoc.result.ProgressInfo;
import apoc.util.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.cypher.export.DatabaseSubGraph;
import org.neo4j.cypher.export.SubGraph;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.TerminationGuard;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static apoc.ApocConfig.APOC_EXPORT_FILE_ENABLED;
import static apoc.ApocConfig.EXPORT_NOT_ENABLED_ERROR;
import static apoc.ApocConfig.apocConfig;
import static apoc.export.parquet.ParquetExportType.Type.from;

@Extended
public class ExportParquet {
    public static final String EXPORT_TO_FILE_PARQUET_ERROR = EXPORT_NOT_ENABLED_ERROR +
            "\nOtherwise, if you are running in a cloud environment without filesystem access, use the apoc.export.parquet.*.stream procedures to stream the export back to your client.";;

    @Context
    public Transaction tx;

    @Context
    public Log log;

    @Context
    public GraphDatabaseService db;

    @Context
    public TerminationGuard terminationGuard;

    @Context
    public ApocConfig apocConfig;

    @Context
    public Pools pools;


    @Procedure("apoc.export.parquet.all.stream")
    @Description("Exports the full database as a Parquet byte array.")
    public Stream<ByteArrayResult> all(@Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        return exportParquet(new DatabaseSubGraph(tx), new ParquetConfig(config));
    }

    @Procedure("apoc.export.parquet.data.stream")
    @Description("Exports the given nodes and relationships as a Parquet byte array.")
    public Stream<ByteArrayResult> data(@Name("nodes") List<Node> nodes, @Name("rels") List<Relationship> rels, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        ParquetConfig conf = new ParquetConfig(config);
        return exportParquet(new NodesAndRelsSubGraph(tx, nodes, rels), conf);
    }

    @Procedure("apoc.export.parquet.graph.stream")
    @Description("Exports the given graph as a Parquet byte array.")
    public Stream<ByteArrayResult> graph(@Name("graph") Map<String,Object> graph, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        Collection<Node> nodes = (Collection<Node>) graph.get("nodes");
        Collection<Relationship> rels = (Collection<Relationship>) graph.get("relationships");
        ParquetConfig conf = new ParquetConfig(config);

        return exportParquet(new NodesAndRelsSubGraph(tx, nodes, rels), conf);
    }

    @Procedure("apoc.export.parquet.query.stream")
    @Description("Exports the given Cypher query as a Parquet byte array.")
    public Stream<ByteArrayResult> query(@Name("query") String query, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        ParquetConfig exportConfig = new ParquetConfig(config);
        Map<String,Object> params = config == null ? Collections.emptyMap() : (Map<String,Object>)config.getOrDefault("params", Collections.emptyMap());
        Result result = tx.execute(query,params);

        return exportParquet(result, exportConfig);
    }

    @Procedure("apoc.export.parquet.all")
    @Description("Exports the full database as a Parquet file.")
    public Stream<ProgressInfo> all(@Name("file") String fileName, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws IOException {
        return exportParquet(fileName, new DatabaseSubGraph(tx), new ParquetConfig(config));
    }

    @Procedure("apoc.export.parquet.data")
    @Description("Exports the given nodes and relationships as a Parquet file.")
    public Stream<ProgressInfo> data(@Name("nodes") List<Node> nodes, @Name("rels") List<Relationship> rels, @Name("file") String fileName, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws IOException {
        ParquetConfig conf = new ParquetConfig(config);
        return exportParquet(fileName, new NodesAndRelsSubGraph(tx, nodes, rels), conf);
    }

    @Procedure("apoc.export.parquet.graph")
    @Description("Exports the given graph as a Parquet file.")
    public Stream<ProgressInfo> graph(@Name("graph") Map<String,Object> graph, @Name("file") String fileName, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws IOException {
        Collection<Node> nodes = (Collection<Node>) graph.get("nodes");
        Collection<Relationship> rels = (Collection<Relationship>) graph.get("relationships");
        ParquetConfig conf = new ParquetConfig(config);

        return exportParquet(fileName, new NodesAndRelsSubGraph(tx, nodes, rels), conf);
    }

    @Procedure("apoc.export.parquet.query")
    @Description("Exports the given Cypher query as a Parquet file.")
    public Stream<ProgressInfo> query(@Name("query") String query, @Name("file") String fileName, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws IOException {
        ParquetConfig exportConfig = new ParquetConfig(config);
        Map<String,Object> params = config == null ? Collections.emptyMap() : (Map<String,Object>)config.getOrDefault("params", Collections.emptyMap());
        Result result = tx.execute(query,params);

        return exportParquet(fileName, result, exportConfig);
    }

    public Stream<ProgressInfo> exportParquet(String fileName, Object data, ParquetConfig config) throws IOException {
        if (StringUtils.isBlank(fileName)) {
            throw new RuntimeException("The fileName must exists. Otherwise, use the `apoc.export.parquet.*.stream.` procedures to stream the export back to your client.");
        }
        // normalize file url
        fileName = FileUtils.changeFileUrlIfImportDirectoryConstrained(fileName);

        // we cannot use apocConfig().checkWriteAllowed(..) because the error is confusing
        //  since it says "... use the `{stream:true}` config", but with arrow procedures the streaming mode is implemented via different procedures
        if (!apocConfig().getBoolean(APOC_EXPORT_FILE_ENABLED)) {
            throw new RuntimeException(EXPORT_TO_FILE_PARQUET_ERROR);
        }
        ParquetExportType exportType = from(data);
        if (data instanceof Result) {
            return new ExportParquetResultFileStrategy(fileName, db, pools, terminationGuard, log, exportType).export((Result) data, config);
        }
        return new ExportParquetGraphFileStrategy(fileName, db, pools, terminationGuard, log, exportType).export((SubGraph) data, config);
    }

    public Stream<ByteArrayResult> exportParquet(Object data, ParquetConfig config) {

        ParquetExportType exportType = from(data);
        if (data instanceof Result) {
            return new ExportParquetResultStreamStrategy(db, pools, terminationGuard, log, exportType).export((Result) data, config);
        }
        return new ExportParquetGraphStreamStrategy(db, pools, terminationGuard, log, exportType).export((SubGraph) data, config);
    }
}

