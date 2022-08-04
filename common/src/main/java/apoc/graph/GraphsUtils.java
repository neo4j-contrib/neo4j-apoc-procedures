package apoc.graph;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.internal.helpers.collection.Iterables;

public class GraphsUtils {
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
}
