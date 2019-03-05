package apoc.graph;

import apoc.cypher.Cypher;
import apoc.graph.document.builder.DocumentToGraph;
import apoc.graph.util.GraphsConfig;
import apoc.result.RowResult;
import apoc.result.VirtualGraph;
import apoc.util.JsonUtil;
import apoc.util.Util;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.procedure.*;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

/**
 * @author mh
 * @since 27.05.16
 */
public class Graphs {

    @Context
    public GraphDatabaseService db;

    @Description("apoc.graph.fromData([nodes],[relationships],'name',{properties}) | creates a virtual graph object for later processing")
    @Procedure
    public Stream<VirtualGraph> fromData(@Name("nodes") List<Node> nodes, @Name("relationships") List<Relationship> relationships, @Name("name") String name, @Name("properties") Map<String,Object> properties) {
        return Stream.of(new VirtualGraph(name,nodes,relationships,properties));
    }
    @Description("apoc.graph.from(data,'name',{properties}) | creates a virtual graph object for later processing it tries its best to extract the graph information from the data you pass in")
    @Procedure
    public Stream<VirtualGraph> from(@Name("data") Object data, @Name("name") String name, @Name("properties") Map<String,Object> properties) {
        Set<Node> nodes = new HashSet<>(1000);
        Set<Relationship> rels = new HashSet<>(10000);
        extract(data, nodes,rels);
        return Stream.of(new VirtualGraph(name,nodes,rels,properties));
    }

    public static boolean extract(Object data, Set<Node> nodes, Set<Relationship> rels) {
        boolean found = false;
        if (data == null) return false;
        if (data instanceof Node) {
            nodes.add((Node)data);
            return true;
        }
        else if (data instanceof Relationship) {
            rels.add((Relationship) data);
            return true;
        }
        else if (data instanceof Path) {
            Iterables.addAll(nodes,((Path)data).nodes());
            Iterables.addAll(rels,((Path)data).relationships());
            return true;
        }
        else if (data instanceof Iterable) {
            for (Object o : (Iterable)data) found |= extract(o,nodes,rels);
        }
        else if (data instanceof Map) {
            for (Object o : ((Map)data).values()) found |= extract(o,nodes,rels);
        }
        else if (data instanceof Iterator) {
            Iterator it = (Iterator) data;
            while (it.hasNext()) found |= extract(it.next(), nodes,rels);
        } else if (data instanceof Object[]) {
            for (Object o : (Object[])data) found |= extract(o,nodes,rels);
        }
        return found;
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

    @Description("apoc.graph.fromCypher('kernelTransaction',{params},'name',{properties}) - creates a virtual graph object for later processing")
    @Procedure
    public Stream<VirtualGraph> fromCypher(@Name("kernelTransaction") String statement,  @Name("params") Map<String,Object> params,@Name("name") String name,  @Name("properties") Map<String,Object> properties) {
        params = params == null ? Collections.emptyMap() : params;
        Set<Node> nodes = new HashSet<>(1000);
        Set<Relationship> rels = new HashSet<>(1000);
        Map<String,Object> props = new HashMap<>(properties);
        db.execute(Cypher.withParamMapping(statement, params.keySet()), params).stream().forEach(row -> {
            row.forEach((k,v) -> {
                if (!extract(v,nodes,rels)) {
                    props.put(k,v);
                }
            });

        });
        return Stream.of(new VirtualGraph(name,nodes,rels,props));
    }

    @Description("apoc.graph.fromDocument({json}, {config}) yield graph - transform JSON documents into graph structures")
    @Procedure(mode = Mode.WRITE)
    public Stream<VirtualGraph> fromDocument(@Name("json") Object document, @Name(value = "config", defaultValue = "{}") Map<String,Object> config) throws Exception {
        Collection<Map> coll = getDocumentCollection(document);
        DocumentToGraph documentToGraph = new DocumentToGraph(db, new GraphsConfig(config));
        return Stream.of(documentToGraph.create(coll));
    }

    public Collection<Map> getDocumentCollection(@Name("json") Object document) {
        Collection<Map> coll;
        if (document instanceof String) {
            document  = JsonUtil.parse((String) document, null, Object.class);
        }
        if (document instanceof Collection) {
            coll = (Collection) document;
        } else {
            coll = Collections.singleton((Map) document);
        }
        return coll;
    }

    @Description("apoc.graph.validateDocument({json}, {config}) yield row - validates the json, return the result of the validation")
    @Procedure(mode = Mode.READ)
    public Stream<RowResult> validateDocument(@Name("json") Object document, @Name(value = "config", defaultValue = "{}") Map<String,Object> config) throws Exception {
        DocumentToGraph documentToGraph = new DocumentToGraph(db, new GraphsConfig(config));
        AtomicLong index = new AtomicLong(-1);
        return getDocumentCollection(document).stream()
                .map(elem -> {
                    try {
                        documentToGraph.validate(elem);
                        return new RowResult(Util.map("index", index.incrementAndGet(), "message", "Valid"));
                    } catch (Exception e) {
                        return new RowResult(Util.map("index", index.incrementAndGet(), "message", e.getMessage()));
                    }
                });
    }
}
