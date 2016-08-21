package apoc.export;

import apoc.Description;
import apoc.export.cypher.ExportCypher;
import apoc.export.cypher.MultiStatementCypherSubGraphExporter;
import apoc.export.util.NodesAndRelsSubGraph;
import apoc.result.ProgressInfo;
import apoc.export.util.ProgressReporter;
import apoc.util.Util;
import org.neo4j.cypher.export.CypherResultSubGraph;
import org.neo4j.cypher.export.DatabaseSubGraph;
import org.neo4j.cypher.export.SubGraph;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Result;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static apoc.export.util.FileUtils.getPrintWriter;
import static apoc.util.Util.toBoolean;
import static apoc.util.Util.toLong;

/**
 * @author mh
 * @since 22.05.16
 */
public class Export {
    @Context
    public GraphDatabaseService db;

    @Procedure
    @Description("apoc.export.cypherAll(file,config) - exports whole database incl. indexes as cypher statements to the provided file")
    public Stream<ProgressInfo> cypherAll(@Name("file") String fileName, @Name("config") Map<String, Object> config) throws Exception {
        return new ExportCypher(db).all(fileName,config);
    }

    @Procedure
    @Description("apoc.export.cypherData(nodes,rels,file,config) - exports given nodes and relationships incl. indexes as cypher statements to the provided file")
    public Stream<ProgressInfo> cypherData(@Name("nodes") List<Node> nodes, @Name("rels") List<Relationship> rels, @Name("file") String fileName, @Name("config") Map<String, Object> config) throws Exception {
        return new ExportCypher(db).data(nodes,rels,fileName,config);
    }
    @Procedure
    @Description("apoc.export.cypherGraph(graph,file,config) - exports given graph object incl. indexes as cypher statements to the provided file")
    public Stream<ProgressInfo> cypherGraph(@Name("graph") Map<String,Object> graph, @Name("file") String fileName, @Name("config") Map<String, Object> config) throws Exception {
        return new ExportCypher(db).graph(graph,fileName,config);
    }

    @Procedure
    @Description("apoc.export.cypherQuery(query,file,config) - exports nodes and relationships from the cypher statement incl. indexes as cypher statements to the provided file")
    public Stream<ProgressInfo> cypherQuery(@Name("query") String query, @Name("file") String fileName, @Name("config") Map<String, Object> config) throws Exception {
        return new ExportCypher(db).query(query,fileName,config);
    }
}
