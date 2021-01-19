package apoc.convert;

import apoc.coll.SetBackedList;
import apoc.meta.Meta.Types;
import apoc.util.Util;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;


/**
 * @author mh
 * @since 29.05.16
 */
public class Convert {

    @Context
    public Log log;

    @UserFunction
    @Description("apoc.convert.toMap(value) | tries it's best to convert the value to a map")
    public Map<String, Object> toMap(@Name("map") Object map) {

        if (map instanceof Entity) {
            return ((Entity)map).getAllProperties();
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
    public static List convertToList(Object list) {
        if (list == null) return null;
        else if (list instanceof List) return (List) list;
        else if (list instanceof Collection) return new ArrayList((Collection)list);
        else if (list instanceof Iterable) return Iterators.addToCollection(((Iterable)list).iterator(),(List)new ArrayList<>(100));
        else if (list instanceof Iterator) return Iterators.addToCollection((Iterator)list,(List)new ArrayList<>(100));
        else if (list.getClass().isArray()) {
            final Object[] objectArray;
            if (list.getClass().getComponentType().isPrimitive()) {
                int length = Array.getLength(list);
                objectArray = new Object[length];
                for (int i = 0; i < length; i++) {
                    objectArray[i] = Array.get(list, i);
                }
            } else {
                objectArray = (Object[]) list;
            }
            List result = new ArrayList<>(objectArray.length);
            Collections.addAll(result, objectArray);
            return result;
        }
        return Collections.singletonList(list);
    }
    
    @SuppressWarnings("unchecked")
    private <T> List<T> convertToList(Object list, Class<T> type) {
        List<Object> convertedList = convertToList(list);
        if (convertedList == null) {
        	return null;
        }
        Stream<T> stream = null;
        Types varType = Types.of(type);
    	switch (varType) {
    	case INTEGER:
    		stream = (Stream<T>) convertedList.stream().map(Util::toLong);
    		break;
    	case FLOAT:
    		stream = (Stream<T>) convertedList.stream().map(Util::toDouble);
    		break;
    	case STRING:
    		stream = (Stream<T>) convertedList.stream().map(this::toString);
    		break;
    	case BOOLEAN:
    		stream = (Stream<T>) convertedList.stream().map(this::toBoolean);
    		break;
    	case NODE:
    		stream = (Stream<T>) convertedList.stream().map(this::toNode);
    		break;
    	case RELATIONSHIP:
    		stream = (Stream<T>) convertedList.stream().map(this::toRelationship);
    		break;
		default:
			throw new RuntimeException("Supported types are: Integer, Float, String, Boolean, Node, Relationship");
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

    @UserFunction
    @Description("apoc.convert.toInteger(value) | tries it's best to convert the value to an integer")
    public Long toInteger(@Name("object") Object obj) {
        if (obj == null || obj.equals("")) {
            return null;
        }

        Types varType = Types.of(obj);
        switch (varType) {
            case INTEGER:
            case FLOAT:
                return ((Number) obj).longValue();
            case STRING:
                return parseLongString((String)obj);
            case BOOLEAN:
                return ((boolean) obj) ? 1L : 0L;
            default:
                return null;
        }
    }

    private Long parseLongString(String input) {
        if (input.equalsIgnoreCase("true")) {
            return 1L;
        }
        if (input.equalsIgnoreCase("false")) {
            return 0L;
        }
        if (input.startsWith("0x")) {
            return Long.valueOf(input.substring(2), 16);
        }
        try {
            return Long.parseLong(input);
        } catch (NumberFormatException nfe) {
            try {
                return (long)Double.parseDouble(input);
            } catch(NumberFormatException nfe2) {}
        }
        return null;
    }

    @UserFunction
    @Description("apoc.convert.toFloat(value) | tries it's best to convert the value to a float")
    public Double toFloat(@Name("object") Object obj) {
        if (obj == null || obj.equals("")) {
            return null;
        }

        Types varType = Types.of(obj);
        switch (varType) {
            case FLOAT:
            case INTEGER:
                return ((Number) obj).doubleValue();
            case STRING:
                return parseDoubleString((String)obj);
            case BOOLEAN:
                return ((boolean) obj) ? 1D : 0D;
            default:
                return null;
        }
    }


    private Double parseDoubleString(String input) {
        if (input.equalsIgnoreCase("true")) {
            return 1D;
        }
        if (input.equalsIgnoreCase("false")) {
            return 0D;
        }
        try {
            if (input.startsWith("0x")) {
                return (double)Long.parseLong(input.substring(2), 16);
            }
            return Double.parseDouble(input);
        } catch (NumberFormatException ex) {
            return null;
        }
    }
}
