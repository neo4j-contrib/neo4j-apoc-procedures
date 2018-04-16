package apoc.export;

import org.neo4j.procedure.Description;
import apoc.export.cypher.ExportCypher;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * @author mh
 * @since 22.05.16
 */
public class Export {
    @Context
    public GraphDatabaseService db;

    @Procedure
    @Deprecated
    @Description("apoc.export.cypherAll(file,config) - exports whole database incl. indexes as cypher statements to the provided file")
    public Stream<ExportCypher.DataProgressInfo> cypherAll(@Name("file") String fileName, @Name("config") Map<String, Object> config) throws Exception {
        return new ExportCypher(db).all(fileName,config);
    }

    @Procedure
    @Deprecated
    @Description("apoc.export.cypherData(nodes,rels,file,config) - exports given nodes and relationships incl. indexes as cypher statements to the provided file")
    public Stream<ExportCypher.DataProgressInfo> cypherData(@Name("nodes") List<Node> nodes, @Name("rels") List<Relationship> rels, @Name("file") String fileName, @Name("config") Map<String, Object> config) throws Exception {
        return new ExportCypher(db).data(nodes,rels,fileName,config);
    }
    @Procedure
    @Deprecated
    @Description("apoc.export.cypherGraph(graph,file,config) - exports given graph object incl. indexes as cypher statements to the provided file")
    public Stream<ExportCypher.DataProgressInfo> cypherGraph(@Name("graph") Map<String,Object> graph, @Name("file") String fileName, @Name("config") Map<String, Object> config) throws Exception {
        return new ExportCypher(db).graph(graph,fileName,config);
    }

    @Procedure
    @Deprecated
    @Description("apoc.export.cypherQuery(query,file,config) - exports nodes and relationships from the cypher kernelTransaction incl. indexes as cypher statements to the provided file")
    public Stream<ExportCypher.DataProgressInfo> cypherQuery(@Name("query") String query, @Name("file") String fileName, @Name("config") Map<String, Object> config) throws Exception {
        return new ExportCypher(db).query(query,fileName,config);
    }
}
