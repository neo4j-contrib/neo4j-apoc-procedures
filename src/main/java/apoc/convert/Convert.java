package apoc.convert;

import org.neo4j.procedure.Description;
import apoc.coll.SetBackedList;
import apoc.result.*;
import apoc.util.Util;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.UserFunction;

import java.lang.reflect.Array;
import java.util.*;
import java.util.stream.Stream;

/**
 * @author mh
 * @since 29.05.16
 */
public class Convert {

    @UserFunction
    @Description("apoc.convert.toMap(value) | tries it's best to convert the value to a map")
    public Map<String, Object> toMap(@Name("map") Object map) {
        return map instanceof Map ? (Map<String, Object>) map :  null;
    }
    @UserFunction
    @Description("apoc.convert.toString(value) | tries it's best to convert the value to a string")
    public String toString(@Name("string") Object string) {
        return string  == null ? null : string.toString();
    }

    @UserFunction
    @Description("apoc.convert.toList(value) | tries it's best to convert the value to a list")
    public List<Object> toList(@Name("list") Object list) {
        return convertToList(list);
    }
    @UserFunction
    @Description("apoc.convert.toBoolean(value) | tries it's best to convert the value to a boolean")
    public Boolean toBoolean(@Name("bool") Object bool) {
        return Util.toBoolean(bool);
    }

    @UserFunction
    @Description("apoc.convert.toNode(value) | tries it's best to convert the value to a node")
    public Node toNode(@Name("node") Object node) {
        return node instanceof Node ? (Node) node :  null;
    }

    @UserFunction
    @Description("apoc.convert.toRelationship(value) | tries it's best to convert the value to a relationship")
    public Relationship toRelationship(@Name("relationship") Object relationship) {
        return relationship instanceof Relationship ? (Relationship) relationship :  null;
    }

    @SuppressWarnings("unchecked")
    private List convertToList(Object list) {
        if (list == null) return null;
        else if (list instanceof List) return (List) list;
        else if (list instanceof Collection) return new ArrayList((Collection)list);
        else if (list instanceof Iterable) return Iterables.addToCollection((Iterable)list,(List)new ArrayList<>(100));
        else if (list instanceof Iterator) return Iterators.addToCollection((Iterator)list,(List)new ArrayList<>(100));
        else if (list.getClass().isArray() && !list.getClass().getComponentType().isPrimitive()) {
            List result = new ArrayList<>(100);
            Collections.addAll(result, ((Object[]) list));
            return result;
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    @UserFunction
    @Description("apoc.convert.toSet(value) | tries it's best to convert the value to a set")
    public List<Object> toSet(@Name("list") Object value) {
        List list = convertToList(value);
        return list == null ? null : new SetBackedList(new LinkedHashSet<>(list));
    }    
}
