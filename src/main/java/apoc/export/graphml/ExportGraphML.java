package apoc.export.graphml;

import apoc.ApocConfig;
import apoc.Pools;
import apoc.export.cypher.ExportFileManager;
import apoc.export.cypher.FileManagerFactory;
import apoc.export.util.ExportConfig;
import apoc.export.util.ExportUtils;
import apoc.export.util.NodesAndRelsSubGraph;
import apoc.export.util.ProgressReporter;
import apoc.result.ProgressInfo;
import apoc.util.FileUtils;
import apoc.util.Util;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.cypher.export.CypherResultSubGraph;
import org.neo4j.cypher.export.DatabaseSubGraph;
import org.neo4j.cypher.export.SubGraph;
import org.neo4j.graphdb.*;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.procedure.*;

import java.io.PrintWriter;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * @author mh
 * @since 22.05.16
 */
public class ExportGraphML {
    @Context
    public GraphDatabaseService db;

    @Context
    public Transaction tx;

    @Context
    public ApocConfig apocConfig;

    @Context
    public Pools pools;

    @Context
    public TerminationGuard terminationGuard;

    @Procedure(name = "apoc.import.graphml",mode = Mode.WRITE)
    @Description("apoc.import.graphml(file,config) - imports graphml file")
    public Stream<ProgressInfo> file(@Name("file") String fileName, @Name("config") Map<String, Object> config) throws Exception {
        ProgressInfo result =
        Util.inThread(pools, () -> {
            ExportConfig exportConfig = new ExportConfig(config);
            ProgressReporter reporter = new ProgressReporter(null, null, new ProgressInfo(fileName, "file", "graphml"));
            XmlGraphMLReader graphMLReader = new XmlGraphMLReader(db, tx).reporter(reporter)
                    .batchSize(exportConfig.getBatchSize())
                    .relType(exportConfig.defaultRelationshipType())
                    .nodeLabels(exportConfig.readLabels());

            if (exportConfig.storeNodeIds()) graphMLReader.storeNodeIds();


            graphMLReader.parseXML(FileUtils.readerFor(fileName));
            return reporter.getTotal();
        });
        return Stream.of(result);
    }
    @Procedure
    @Description("apoc.export.graphml.all(file,config) - exports whole database as graphml to the provided file")
    public Stream<ProgressInfo> all(@Name("file") String fileName, @Name("config") Map<String, Object> config) throws Exception {

        String source = String.format("database: nodes(%d), rels(%d)", Util.nodeCount(tx), Util.relCount(tx));
        return exportGraphML(fileName, source, new DatabaseSubGraph(tx), new ExportConfig(config));
    }

    @Procedure
    @Description("apoc.export.graphml.data(nodes,rels,file,config) - exports given nodes and relationships as graphml to the provided file")
    public Stream<ProgressInfo> data(@Name("nodes") List<Node> nodes, @Name("rels") List<Relationship> rels, @Name("file") String fileName, @Name("config") Map<String, Object> config) throws Exception {

        String source = String.format("data: nodes(%d), rels(%d)", nodes.size(), rels.size());
        return exportGraphML(fileName, source, new NodesAndRelsSubGraph(tx, nodes, rels), new ExportConfig(config));
    }
    @Procedure
    @Description("apoc.export.graphml.graph(graph,file,config) - exports given graph object as graphml to the provided file")
    public Stream<ProgressInfo> graph(@Name("graph") Map<String,Object> graph, @Name("file") String fileName, @Name("config") Map<String, Object> config) throws Exception {

        Collection<Node> nodes = (Collection<Node>) graph.get("nodes");
        Collection<Relationship> rels = (Collection<Relationship>) graph.get("relationships");
        String source = String.format("graph: nodes(%d), rels(%d)", nodes.size(), rels.size());
        return exportGraphML(fileName, source, new NodesAndRelsSubGraph(tx, nodes, rels), new ExportConfig(config));
    }

    @Procedure
    @Description("apoc.export.graphml.query(query,file,config) - exports nodes and relationships from the cypher statement as graphml to the provided file")
    public Stream<ProgressInfo> query(@Name("query") String query, @Name("file") String fileName, @Name("config") Map<String, Object> config) throws Exception {
        ExportConfig c = new ExportConfig(config);
        Result result = tx.execute(query);
        SubGraph graph = CypherResultSubGraph.from(tx, result, c.getRelsInBetween());
        String source = String.format("statement: nodes(%d), rels(%d)",
                Iterables.count(graph.getNodes()), Iterables.count(graph.getRelationships()));
        return exportGraphML(fileName, source, graph, c);
    }

    private Stream<ProgressInfo> exportGraphML(@Name("file") String fileName, String source, SubGraph graph, ExportConfig exportConfig) throws Exception {
        if (StringUtils.isNotBlank(fileName)) apocConfig.checkWriteAllowed(exportConfig);
        final String format = "graphml";
        ProgressReporter reporter = new ProgressReporter(null, null, new ProgressInfo(fileName, source, format));
        XmlGraphMLWriter exporter = new XmlGraphMLWriter();
        ExportFileManager cypherFileManager = FileManagerFactory.createFileManager(fileName, false);
        final PrintWriter graphMl = cypherFileManager.getPrintWriter(format);
        if (exportConfig.streamStatements()) {
            return ExportUtils.getProgressInfoStream(db, pools.getDefaultExecutorService() ,terminationGuard, format, exportConfig, reporter, cypherFileManager,
                    (reporterWithConsumer) -> {
                        try {
                            exporter.write(graph, graphMl, reporterWithConsumer, exportConfig);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    });
        } else {
            exporter.write(graph, graphMl, reporter, exportConfig);
            closeWriter(graphMl);
            return reporter.stream();
        }
    }

    private void closeWriter(PrintWriter writer) {
        writer.flush();
        try {
            writer.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
