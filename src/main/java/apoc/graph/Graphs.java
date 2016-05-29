package apoc.graph;

import apoc.Description;
import apoc.cypher.Cypher;
import apoc.result.VirtualGraph;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.*;
import java.util.stream.Stream;

/**
 * @author mh
 * @since 27.05.16
 */
public class Graphs {

    @Context
    public GraphDatabaseService db;

    @Description("apoc.graph.fromData([nodes],[relationships],'name',{properties}) - creates a virtual graph object for later processing")
    @Procedure
    public Stream<VirtualGraph> fromData(@Name("nodes") List<Node> nodes, @Name("relationships") List<Relationship> relationships, @Name("name") String name, @Name("properties") Map<String,Object> properties) {
        return Stream.of(new VirtualGraph(name,nodes,relationships,properties));
    }

    @Description("apoc.graph.fromPaths(path,'name',{properties}) - creates a virtual graph object for later processing")
    @Procedure
    public Stream<VirtualGraph> fromPath(@Name("path") Path paths, @Name("name") String name, @Name("properties") Map<String,Object> properties) {
        return Stream.of(new VirtualGraph(name, paths.nodes(), paths.relationships(),properties));
    }

    @Description("apoc.graph.fromPaths([paths],'name',{properties}) - creates a virtual graph object for later processing")
    @Procedure
    public Stream<VirtualGraph> fromPaths(@Name("paths") List<Path> paths, @Name("name") String name, @Name("properties") Map<String,Object> properties) {
        List<Node> nodes = new ArrayList<>(1000);
        List<Relationship> rels = new ArrayList<>(1000);
        for (Path path : paths) {
            Iterables.addAll(nodes,path.nodes());
            Iterables.addAll(rels,path.relationships());
        }
        return Stream.of(new VirtualGraph(name,nodes,rels,properties));
    }

    @Description("apoc.graph.fromDB('name',{properties}) - creates a virtual graph object for later processing")
    @Procedure
    public Stream<VirtualGraph> fromDB(@Name("name") String name, @Name("properties") Map<String,Object> properties) {
        return Stream.of(new VirtualGraph(name,db.getAllNodes(),db.getAllRelationships(),properties));
    }

    @Description("apoc.graph.fromCypher('statement',{params},'name',{properties}) - creates a virtual graph object for later processing")
    @Procedure
    public Stream<VirtualGraph> fromCypher(@Name("statement") String statement,  @Name("params") Map<String,Object> params,@Name("name") String name,  @Name("properties") Map<String,Object> properties) {
        params = params == null ? Collections.emptyMap() : params;
        List<Node> nodes = new ArrayList<>(1000);
        List<Relationship> rels = new ArrayList<>(1000);
        Map<String,Object> props = new HashMap<>(properties);
        db.execute(Cypher.compiled(statement, params.keySet()), params).stream().forEach( row -> {
            row.forEach((k,v) -> {
                if (!consume(v,nodes,rels)) {
                    props.put(k,v);
                }
            });

        });
        return Stream.of(new VirtualGraph(name,nodes,rels,props));
    }

    // todo keep non-graph stuff around and return it
    private boolean consume(Object v, List<Node> nodes, List<Relationship> rels) {
        if (v instanceof Node) {
            nodes.add((Node)v);
            return true;
        }
        if (v instanceof Relationship) {
            rels.add((Relationship)v);
            return true;
        }
        if (v instanceof Iterable) {
            boolean found = false;
            for (Object o : ((Iterable)v)) {
                found |= consume(o,nodes,rels);
            }
            if (found) return true;
        }
        if (v instanceof Map) {
            boolean found = false;
            for (Map.Entry<String,Object> e : ((Map<String,Object>)v).entrySet()) {
                found |= consume(e.getValue(),nodes,rels);
            }
            if (found) return true;
        }
        return false;
    }

}
