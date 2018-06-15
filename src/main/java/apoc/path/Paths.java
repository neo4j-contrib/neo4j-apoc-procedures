package apoc.path;

import apoc.Description;
import org.neo4j.driver.internal.util.Iterables;
import org.neo4j.graphalgo.impl.util.PathImpl;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

import java.util.Iterator;
import java.util.List;

/**
 * @author mh
 * @since 19.02.18
 */
public class Paths {

    @UserFunction
    @Description("apoc.path.create(startNode,[rels]) - creates a path instance of the given elements")
    public Path create(@Name("startNode") Node startNode, @Name(value = "rels", defaultValue = "[]") List<Relationship> rels) {
        if (startNode == null) return null;
        PathImpl.Builder builder = new PathImpl.Builder(startNode);
        if (rels != null) {
            for (Relationship rel : rels) {
                builder = builder.push(rel);
            }
        }
        return builder.build();
    }

    @UserFunction
    @Description("apoc.path.slice(path, [offset], [length]) - creates a sub-path with the given offset and length")
    public Path slice(@Name("path") Path path, @Name(value = "offset", defaultValue = "0") long offset,@Name(value = "length", defaultValue = "-1") long length) {
        if (path == null) return null;
        if (offset < 0) offset = 0;
        if (length == -1) length = path.length() - offset;
        if (offset == 0 && length >= path.length()) return path;

        Iterator<Node> nodes = path.nodes().iterator();
        Iterator<Relationship> rels = path.relationships().iterator();
        for (long i = 0; i < offset && nodes.hasNext() && rels.hasNext(); i++) {
            nodes.next();
            rels.next();
        }
        if (!nodes.hasNext()) return null;

        PathImpl.Builder builder = new PathImpl.Builder(nodes.next());
        for (long i = 0; i < length && rels.hasNext(); i++) {
            builder = builder.push(rels.next());
        }
        return builder.build();
    }

    @UserFunction
    @Description("apoc.path.combine(path1, path2) - combines the paths into one if the connecting node matches")
    public Path combine(@Name("first") Path first, @Name("second") Path second) {
        if (first == null) return second;
        if (second == null) return first;

        if (!first.endNode().equals(second.startNode()))
            throw new IllegalArgumentException("Paths don't connect on their end and start-nodes "+first+ " with "+second);

        PathImpl.Builder builder = new PathImpl.Builder(first.startNode());
        for (Relationship rel : first.relationships()) builder = builder.push(rel);
        for (Relationship rel : second.relationships()) builder = builder.push(rel);
        return builder.build();
    }

    @UserFunction
    @Description("apoc.path.elements(path) - returns a list of node-relationship-node-...")
    public List<Object> elements(@Name("path") Path path) {
        if (path == null) return null;
        return Iterables.asList((Iterable)path);
    }
}
