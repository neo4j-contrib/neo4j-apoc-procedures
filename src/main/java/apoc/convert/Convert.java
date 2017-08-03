package apoc.convert;

import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

import apoc.coll.SetBackedList;
import apoc.number.exact.Exact;
import apoc.util.Util;


/**
 * @author mh
 * @since 29.05.16
 */
public class Convert {

    @UserFunction
    @Description("apoc.convert.toMap(value) | tries it's best to convert the value to a map")
    public Map<String, Object> toMap(@Name("map") Object map) {

        if (map instanceof PropertyContainer) {
            return ((PropertyContainer)map).getAllProperties();
        } else if (map instanceof Map) {
            return (Map<String, Object>) map;
        } else {
            return null;
        }
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
    private <T> List<T> convertToList(Object list, Class<T> type) {
        List<Object> convertedList = convertToList(list);
        if (convertedList == null) {
        	return null;
        }
        Stream<T> stream = null;
    	if (type.isAssignableFrom(Long.class)) {
    		final Exact exact = new Exact();
    		stream = (Stream<T>) convertedList.stream().map(e -> {
    			if (e == null) {
    				return null;
    			}
    			if (e instanceof Number) {
    				return ((Number)e).longValue();
    			}
    			return exact.toInteger(toString(e), 0L, RoundingMode.HALF_UP.toString());
    		});
    	} else if (type.isAssignableFrom(Double.class)) {
    		final Exact exact = new Exact();
    		stream = (Stream<T>) convertedList.stream().map(e -> {
				if (e == null) {
    				return null;
    			}
    			if (e instanceof Number) {
    				return ((Number)e).doubleValue();
    			}
    			return exact.toFloat(toString(e), 0L, RoundingMode.HALF_UP.toString());
    		});
    	} else if (type.isAssignableFrom(String.class)) {
    		stream = (Stream<T>) convertedList.stream().map(this::toString);
    	} else if (type.isAssignableFrom(Boolean.class)) {
    		stream = (Stream<T>) convertedList.stream().map(this::toBoolean);
    	} else if (type.isAssignableFrom(Node.class)) {
    		stream = (Stream<T>) convertedList.stream().map(this::toNode);
    	} else if (type.isAssignableFrom(Relationship.class)) {
    		stream = (Stream<T>) convertedList.stream().map(this::toRelationship);
    	} else {
    		throw new UnsupportedTypeException("Supported types are: Long, Double, String, Boolean, Node, Relationship");
    	}
    	return stream.collect(Collectors.toList());
    }

	@SuppressWarnings("unchecked")
    @UserFunction
    @Description("apoc.convert.toSet(value) | tries it's best to convert the value to a set")
    public List<Object> toSet(@Name("list") Object value) {
        List list = convertToList(value);
        return list == null ? null : new SetBackedList(new LinkedHashSet<>(list));
    }
    
	@UserFunction
    @Description("apoc.convert.toIntList(value) | tries it's best to convert "
    		+ "the value to a list of integers")
    public List<Long> toIntList(@Name("list") Object list) {
        return convertToList(list, Long.class);
    }

	@UserFunction
	@Description("apoc.convert.toStringList(value) | tries it's best to convert "
			+ "the value to a list of strings")
	public List<String> toStringList(@Name("list") Object list) {
        return convertToList(list, String.class);
	}

	@UserFunction
	@Description("apoc.convert.toBooleanList(value) | tries it's best to convert "
			+ "the value to a list of booleans")
	public List<Boolean> toBooleanList(@Name("list") Object list) {
        return convertToList(list, Boolean.class);
	}

	@UserFunction
	@Description("apoc.convert.toNodeList(value) | tries it's best to convert "
			+ "the value to a list of nodes")
	public List<Node> toNodeList(@Name("list") Object list) {
        return convertToList(list, Node.class);
	}

	@UserFunction
	@Description("apoc.convert.toRelationshipList(value) | tries it's best to convert "
			+ "the value to a list of relationships")
	public List<Relationship> toRelationshipList(@Name("list") Object list) {
        return convertToList(list, Relationship.class);
	}
	
}
