package apoc.export.graphml;

import apoc.Description;
import apoc.export.util.ExportConfig;
import apoc.export.util.FileUtils;
import apoc.export.util.NodesAndRelsSubGraph;
import apoc.export.util.ProgressReporter;
import apoc.result.ProgressInfo;
import apoc.util.Util;
import org.neo4j.cypher.export.CypherResultSubGraph;
import org.neo4j.cypher.export.DatabaseSubGraph;
import org.neo4j.cypher.export.SubGraph;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Result;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.PerformsWrites;
import org.neo4j.procedure.Procedure;

import javax.xml.stream.XMLStreamException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static apoc.export.util.FileUtils.getPrintWriter;

/**
 * @author mh
 * @since 22.05.16
 */
public class ExportGraphML {
    @Context
    public GraphDatabaseAPI db;

    @Procedure("apoc.import.graphml")
    @PerformsWrites
    @Description("apoc.import.graphml(file,config) - imports graphml file")
    public Stream<ProgressInfo> file(@Name("file") String fileName, @Name("config") Map<String, Object> config) throws Exception {
        ProgressInfo result =
        Util.inThread(() -> {
            ExportConfig exportConfig = new ExportConfig(config);
            ProgressReporter reporter = new ProgressReporter(null, null, new ProgressInfo(fileName, "file", "graphml"));
            XmlGraphMLReader graphMLReader = new XmlGraphMLReader(db).reporter(reporter)
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

        String source = String.format("database: nodes(%d), rels(%d)", Util.nodeCount(db), Util.relCount(db));
        return exportGraphML(fileName, source, new DatabaseSubGraph(db), new ExportConfig(config));
    }

    @Procedure
    @Description("apoc.export.graphml.data(nodes,rels,file,config) - exports given nodes and relationships as graphml to the provided file")
    public Stream<ProgressInfo> data(@Name("nodes") List<Node> nodes, @Name("rels") List<Relationship> rels, @Name("file") String fileName, @Name("config") Map<String, Object> config) throws Exception {

        String source = String.format("data: nodes(%d), rels(%d)", nodes.size(), rels.size());
        return exportGraphML(fileName, source, new NodesAndRelsSubGraph(db, nodes, rels), new ExportConfig(config));
    }
    @Procedure
    @Description("apoc.export.graphml.graph(graph,file,config) - exports given graph object as graphml to the provided file")
    public Stream<ProgressInfo> graph(@Name("graph") Map<String,Object> graph, @Name("file") String fileName, @Name("config") Map<String, Object> config) throws Exception {

        Collection<Node> nodes = (Collection<Node>) graph.get("nodes");
        Collection<Relationship> rels = (Collection<Relationship>) graph.get("relationships");
        String source = String.format("graph: nodes(%d), rels(%d)", nodes.size(), rels.size());
        return exportGraphML(fileName, source, new NodesAndRelsSubGraph(db, nodes, rels), new ExportConfig(config));
    }

    @Procedure
    @Description("apoc.export.graphml.query(query,file,config) - exports nodes and relationships from the cypher statement as graphml to the provided file")
    public Stream<ProgressInfo> query(@Name("query") String query, @Name("file") String fileName, @Name("config") Map<String, Object> config) throws Exception {
        ExportConfig c = new ExportConfig(config);
        Result result = db.execute(query);
        SubGraph graph = CypherResultSubGraph.from(result, db, c.getRelsInBetween());
        String source = String.format("statement: nodes(%d), rels(%d)",
                Iterables.count(graph.getNodes()), Iterables.count(graph.getRelationships()));
        return exportGraphML(fileName, source, graph, c);
    }

    private Stream<ProgressInfo> exportGraphML(@Name("file") String fileName, String source, SubGraph graph, ExportConfig config) throws Exception, XMLStreamException {
        ProgressReporter reporter = new ProgressReporter(null, null, new ProgressInfo(fileName, source, "graphml"));
        PrintWriter printWriter = getPrintWriter(fileName, null);
        XmlGraphMLWriter exporter = new XmlGraphMLWriter();
        exporter.write(graph, printWriter, reporter, config);
        printWriter.flush();
        printWriter.close();
        return reporter.stream();
    }
}
