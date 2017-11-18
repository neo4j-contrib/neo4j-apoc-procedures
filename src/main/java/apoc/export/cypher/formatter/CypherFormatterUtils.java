package apoc.export.cypher.formatter;

import apoc.export.util.FormatUtils;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.helpers.collection.Iterables;

import java.lang.reflect.Array;
import java.util.*;

/**
 * @author AgileLARUS
 *
 * @since 16-06-2017
 */
public class CypherFormatterUtils {

	public final static String UNIQUE_ID_LABEL = "UNIQUE IMPORT LABEL";
	public final static String UNIQUE_ID_PROP = "UNIQUE IMPORT ID";
	public final static String Q_UNIQUE_ID_LABEL = quote(UNIQUE_ID_LABEL);

    // ---- node id ----

    public static  String formatNodeLookup(String id, Node node, Map<String, String> uniqueConstraints, Set<String> indexNames) {
        StringBuilder result = new StringBuilder(100);
        result.append("(");
        result.append(id);
        result.append(getNodeIdLabels(node, uniqueConstraints, indexNames));
        Map<String, Object> nodeIdProperties = getNodeIdProperties(node, uniqueConstraints);
        if (nodeIdProperties.size() > 0) {
            result.append("{");
            StringBuilder props = new StringBuilder(100);
            for (String prop : nodeIdProperties.keySet()) {
                props.append(", ");
                props.append(quote(prop));
                props.append(":");
                props.append(CypherFormatterUtils.toString(nodeIdProperties.get(prop)));
            }
            result.append(props.substring(2));
            result.append("}");
        }
        result.append(")");
        return result.toString();
    }

    private static Map<String, Object> getNodeIdProperties(Node node, Map<String, String> uniqueConstraints) {
        Map<String, Object> nodeIdProperties = new LinkedHashMap<>();
        boolean uniqueLabelFound = false;
        for (Label label : node.getLabels()) {
            String prop = uniqueConstraints.get(label.name());
            if (prop != null && node.hasProperty(prop)) {
                uniqueLabelFound = true;
                nodeIdProperties.put(prop, node.getProperty(prop));
            }
        }
        if (!uniqueLabelFound) {
            nodeIdProperties.put(UNIQUE_ID_PROP, node.getId());
        }
        return nodeIdProperties;
    }

    // ---- labels ----

    public static String formatAllLabels(Node node, Map<String, String> uniqueConstraints, Set<String> indexNames) {
        StringBuilder result = new StringBuilder(100);
        boolean uniqueLabelFound = false;
        for (Label label : node.getLabels()) {
            String labelName = label.name();
            String prop = uniqueConstraints.get(labelName);
            if (prop != null && node.hasProperty(prop))
                uniqueLabelFound = true;
            if (indexNames != null && indexNames.contains(labelName))
                result.insert(0, label(labelName));
            else
                result.append(label(labelName));
        }
        if (!uniqueLabelFound) {
            result.append(label(UNIQUE_ID_LABEL));
        }
        return result.toString();
    }

    public static String formatNotUniqueLabels(String id, Node node, Map<String, String> uniqueConstraints) {
        StringBuilder result = new StringBuilder(100);
        for (Label label : node.getLabels()) {
            String prop = uniqueConstraints.get(label.name());
            if (!node.hasProperty(prop)) {
                result.append(", ");
                result.append(id);
                result.append(label(label.name()));
            }
        }
        return result.length() > 0 ? result.substring(2) : "";
    }

    private static String getNodeIdLabels(Node node, Map<String, String> uniqueConstraints, Set<String> indexNames) {
        StringBuilder result = new StringBuilder(100);
        boolean uniqueLabelFound = false;
        for (Label label : node.getLabels()) {
            String labelName = label.name();
            String prop = uniqueConstraints.get(labelName);
            if (prop != null && node.hasProperty(prop)) {
                uniqueLabelFound = true;
                if (indexNames != null && indexNames.contains(labelName)) {
                    result.insert(0, label(labelName));
                }
                else
                    result.append(label(labelName));
            }
        }
        if (!uniqueLabelFound) {
            result.append(label(UNIQUE_ID_LABEL));
        }
        return result.toString();
    }

    // ---- properties ----

    public static String formatNodeProperties(String id, Node node, Map<String, String> uniqueConstraints, Set<String> indexNames, boolean jsonStyle) {
        StringBuilder result = formatProperties(id, node.getAllProperties(), jsonStyle);
        if (getNodeIdLabels(node, uniqueConstraints, indexNames).endsWith(label(UNIQUE_ID_LABEL))) {
            result.append(", ");
            result.append(formatPropertyName(id, UNIQUE_ID_PROP, node.getId(), jsonStyle));
        }
        return result.length() > 0 ? result.substring(2) : "";
    }

    public static String formatRelationshipProperties(String id, Relationship relationship, boolean jsonStyle) {
        StringBuilder result = formatProperties(id, relationship.getAllProperties(), jsonStyle);
        return result.length() > 0 ? result.substring(2) : "";
    }

    public static String formatNotUniqueProperties(String id, Node node, Map<String, String> uniqueConstraints, Set<String> indexedProperties, boolean jsonStyle) {
        Map<String, Object> properties = new LinkedHashMap<>();
        List<String> keys = Iterables.asList(node.getPropertyKeys());
        Collections.sort(keys);
        Map<String, Object> nodeIdProperties = getNodeIdProperties(node, uniqueConstraints);
        for (String prop : keys) {
            if (!nodeIdProperties.containsKey(prop) && indexedProperties.contains(prop))
                properties.put(prop, node.getProperty(prop));
        }
        for (String prop : keys) {
            if (!nodeIdProperties.containsKey(prop) && !indexedProperties.contains(prop))
                properties.put(prop, node.getProperty(prop));
        }
        StringBuilder result = new StringBuilder(100);
        for (String key : properties.keySet()) {
            result.append(", ");
            result.append(formatPropertyName(id, key, properties.get(key), jsonStyle));
        }
        return result.length() > 0 ? result.substring(2) : "";
    }

    private static StringBuilder formatProperties(String id, Map<String, Object> properties, boolean jsonStyle) {
        StringBuilder result = new StringBuilder(100);
        List<String> keys = Iterables.asList(properties.keySet());
        Collections.sort(keys);
        for (String prop : keys) {
            result.append(", ");
            result.append(formatPropertyName(id, prop, properties.get(prop), jsonStyle));
        }
        return result;
    }

    private static String formatPropertyName(String id, String prop, Object value, boolean jsonStyle) {
        return (id != null && !"".equals(id) ? id + "." : "") + "`" + prop + "`" + (jsonStyle ? ":" : "=" ) + toString(value);
    }

	// ---- to string ----

    public static String quote(Iterable<String> ids) {
        StringBuilder builder = new StringBuilder();
        for (Iterator<String> iterator = ids.iterator(); iterator.hasNext(); ) {
            String id = iterator.next();
            builder.append(quote(id));
            if (iterator.hasNext()) {
                builder.append(",");
            }
        }
        return builder.toString();
    }

    public static String quote(String id) {
        return "`" + id + "`";
    }

    public static String label(String id) {
        return ":" + quote(id);
    }

    public static String toString(Object value) {
		if (value == null) return "null";
		if (value instanceof String) return FormatUtils.formatString(value);
		if (value instanceof Number) {
			return FormatUtils.formatNumber((Number) value);
		}
		if (value instanceof Boolean) return value.toString();
		if (value instanceof Iterator) {
			return toString(((Iterator) value));
		}
		if (value instanceof Iterable) {
			return toString(((Iterable) value).iterator());
		}
		if (value.getClass().isArray()) {
			return arrayToString(value);
		}
		return value.toString();
	}

    public static String toString(Iterator<?> iterator) {
        StringBuilder result = new StringBuilder();
        while (iterator.hasNext()) {
            if (result.length() > 0) result.append(", ");
            Object value = iterator.next();
            result.append(toString(value));
        }
        return "[" + result + "]";
    }

    public static String arrayToString(Object value) {
		int length = Array.getLength(value);
		StringBuilder result = new StringBuilder(10 * length);
		for (int i = 0; i < length; i++) {
			if (i > 0) result.append(", ");
			result.append(toString(Array.get(value, i)));
		}
		return "[" + result.toString() + "]";
	}
}
