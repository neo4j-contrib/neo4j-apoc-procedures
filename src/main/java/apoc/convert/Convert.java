package apoc.convert;

import apoc.Description;
import apoc.coll.SetBackedList;
import apoc.result.*;
import apoc.util.Util;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.lang.reflect.Array;
import java.util.*;
import java.util.stream.Stream;

/**
 * @author mh
 * @since 29.05.16
 */
public class Convert {

    @Procedure
    @Description("apoc.convert.toMap(value) | tries it's best to convert the value to a map")
    public Stream<MapResult> toMap(@Name("map") Object map) {
        return Stream.of(new MapResult(map instanceof Map ? (Map) map :  null));
    }
    @Procedure
    @Description("apoc.convert.toString(value) | tries it's best to convert the value to a string")
    public Stream<StringResult> toString(@Name("string") Object string) {
        return Stream.of(new StringResult(string  == null ? null : string.toString()));
    }

    @Procedure
    @Description("apoc.convert.toList(value) | tries it's best to convert the value to a list")
    public Stream<ListResult> toList(@Name("list") Object list) {
        return Stream.of(new ListResult(convertToList(list)));
    }
    @Procedure
    @Description("apoc.convert.toBoolean(value) | tries it's best to convert the value to a boolean")
    public Stream<BooleanResult> toBoolean(@Name("bool") Object bool) {
        return Stream.of(new BooleanResult(Util.toBoolean(bool)));
    }

    @Procedure
    @Description("apoc.convert.toNode(value) | tries it's best to convert the value to a node")
    public Stream<NodeResult> toNode(@Name("node") Object node) {
        return Stream.of(new NodeResult(node instanceof Node ? (Node) node :  null));
    }

    @Procedure
    @Description("apoc.convert.toRelationship(value) | tries it's best to convert the value to a relationship")
    public Stream<RelationshipResult> toRelationship(@Name("relationship") Object relationship) {
        return Stream.of(new RelationshipResult(relationship instanceof Relationship ? (Relationship) relationship :  null));
    }

    @SuppressWarnings("unchecked")
    private List convertToList(Object list) {
        if (list == null) return null;
        else if (list instanceof List) return (List) list;
        else if (list instanceof Collection) return new ArrayList((Collection)list);
        else if (list instanceof Iterable) return Iterables.addToCollection((Iterable)list,new ArrayList<>(100));
        else if (list instanceof Iterator) return Iterators.addToCollection((Iterator)list,new ArrayList<>(100));
        else if (list.getClass().isArray() && !list.getClass().getComponentType().isPrimitive()) {
            List result = new ArrayList<>(100);
            Collections.addAll(result, ((Object[]) list));
            return result;
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    @Procedure
    @Description("apoc.convert.toSet(value) | tries it's best to convert the value to a set")
    public Stream<ListResult> toSet(@Name("list") Object value) {
        List list = convertToList(value);
        return Stream.of(new ListResult(list == null ? null : new SetBackedList(new LinkedHashSet<>(list))));
    }    
}
