package apoc.export.graphml;

import apoc.export.cypher.ExportFileManager;
import apoc.export.util.ExportConfig;
import apoc.export.util.Format;
import apoc.export.util.Reporter;
import apoc.result.ProgressInfo;
import org.neo4j.cypher.export.SubGraph;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;

import java.io.Reader;

/**
 * @author mh
 * @since 21.01.14
 */
public class XmlGraphMLFormat implements Format {
    private final GraphDatabaseService db;

    public XmlGraphMLFormat(GraphDatabaseService db) {
        this.db = db;
    }

    @Override
    public ProgressInfo load(Reader reader, Reporter reporter, ExportConfig config) throws Exception {
        return null;
    }

    @Override
    public ProgressInfo dump(SubGraph graph, ExportFileManager writer, Reporter reporter, ExportConfig config) throws Exception {
        try (Transaction tx = db.beginTx()) {
            XmlGraphMLWriter graphMlWriter = new XmlGraphMLWriter();
            graphMlWriter.write(graph, writer.getPrintWriter("graphml"), reporter, config);
            tx.commit();
        }
        return reporter.getTotal();
    }
}

