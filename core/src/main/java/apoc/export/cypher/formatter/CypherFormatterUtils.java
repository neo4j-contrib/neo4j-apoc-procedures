/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package apoc.export.cypher.formatter;

import apoc.export.util.FormatUtils;
import apoc.util.Util;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import java.lang.reflect.Array;
import java.time.temporal.Temporal;
import java.util.*;
import java.util.stream.Collectors;

import static apoc.export.util.FormatUtils.getLabelsSorted;

/**
 * @author AgileLARUS
 *
 * @since 16-06-2017
 */
public class CypherFormatterUtils {

    public final static String UNIQUE_ID_LABEL = "UNIQUE IMPORT LABEL";
    public final static String UNIQUE_ID_PROP = "UNIQUE IMPORT ID";
    public final static String Q_UNIQUE_ID_LABEL = quote(UNIQUE_ID_LABEL);
    public final static String UNIQUE_ID_REL = "UNIQUE IMPORT ID REL";
    public final static String Q_UNIQUE_ID_REL = quote(UNIQUE_ID_REL);

    public final static String FUNCTION_TEMPLATE = "%s('%s')";

    // ---- node id ----

    public static  String formatNodeLookup(String id, Node node, Map<String, Set<String>> uniqueConstraints, Set<String> indexNames) {
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

    public static Map<String, Object> getNodeIdProperties(Node node, Map<String, Set<String>> uniqueConstraints) {
        Map<String, Object> nodeIdProperties = new LinkedHashMap<>();
        List<String> list = getLabelsSorted(node);

        for (String labelName : list) {
            if (!isUniqueLabelFound(node, uniqueConstraints, labelName)) {
                continue;
            }
            uniqueConstraints.get(labelName).forEach(prop -> {
                nodeIdProperties.put(prop, node.getProperty(prop));
            });
        }
        if (nodeIdProperties.isEmpty()) {
            nodeIdProperties.put(UNIQUE_ID_PROP, node.getId());
        }
        return nodeIdProperties;
    }

    // ---- labels ----

    public static String formatAllLabels(Node node, Map<String, Set<String>> uniqueConstraints, Set<String> indexNames) {
        StringBuilder result = new StringBuilder(100);
        boolean uniqueLabelFound = false;
        List<String> list = getLabelsSorted(node);

        for (String labelName : list) {
            if (!uniqueLabelFound) {
                uniqueLabelFound = isUniqueLabelFound(node, uniqueConstraints, labelName);
            }
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

    public static String formatNotUniqueLabels(String id, Node node, Map<String, Set<String>> uniqueConstraints) {
        StringBuilder result = new StringBuilder(100);
        List<String> list = getLabelsSorted(node);

        for (String labelName : list) {
            if (!isUniqueLabelFound(node, uniqueConstraints, labelName)) {
                result.append(", ");
                result.append(id);
                result.append(label(labelName));
            }
        }
        return formatToString(result);
    }

    private static String getNodeIdLabels(Node node, Map<String, Set<String>> uniqueConstraints, Set<String> indexNames) {
        StringBuilder result = new StringBuilder(100);
        List<String> list = getLabelsSorted(node).stream()
                .filter(labelName -> isUniqueLabelFound(node, uniqueConstraints, labelName))
                .collect(Collectors.toList());
        if (list.isEmpty()) {
            result.append(label(UNIQUE_ID_LABEL));
        } else {
            list.forEach(labelName -> {
                if (indexNames != null && indexNames.contains(labelName)) {
                    result.insert(0, label(labelName));
                } else {
                    result.append(label(labelName));
                }
            });
        }
        return result.toString();
    }

    public static boolean isUniqueLabelFound(Node node, Map<String, Set<String>> uniqueConstraints, String labelName) {
        if (uniqueConstraints.containsKey(labelName)) {
            Set<String> nodeUniqueConstraint = uniqueConstraints.get(labelName);
            return nodeUniqueConstraint.stream().allMatch(node::hasProperty);
        } else {
            return false;
        }
    }

    // ---- properties ----

    public static String formatNodeProperties(String id, Node node, Map<String, Set<String>> uniqueConstraints, Set<String> indexNames, boolean jsonStyle) {
        StringBuilder result = formatProperties(id, node.getAllProperties(), jsonStyle);
        if (getNodeIdLabels(node, uniqueConstraints, indexNames).endsWith(label(UNIQUE_ID_LABEL))) {
            result.append(", ");
            result.append(formatPropertyName(id, UNIQUE_ID_PROP, node.getId(), jsonStyle));
        }
        return formatToString(result);
    }

    public static String formatRelationshipProperties(String id, Relationship relationship, boolean jsonStyle) {
        StringBuilder result = formatProperties(id, relationship.getAllProperties(), jsonStyle);
        return formatToString(result);
    }

    public static String formatNotUniqueProperties(String id, Node node, Map<String, Set<String>> uniqueConstraints, Set<String> indexedProperties, boolean jsonStyle) {
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
        return formatToString(result);
    }

    public static String formatToString(StringBuilder result) {
        return result.length() > 0 ? result.substring(2) : "";
    }

    public static StringBuilder formatProperties(Map<String, Object> properties) {
        return formatProperties("", properties, true);
    }

    public static StringBuilder formatProperties(String id, Map<String, Object> properties, boolean jsonStyle) {
        StringBuilder result = new StringBuilder(100);
        if (properties != null && !properties.isEmpty()) {
            List<String> keys = Iterables.asList(properties.keySet());
            Collections.sort(keys);
            for (String prop : keys) {
                result.append(", ");
                result.append(formatPropertyName(id, prop, properties.get(prop), jsonStyle));
            }
        }
        return result;
    }

    public static String formatPropertyName(String id, String prop, Object value, boolean jsonStyle) {
        return (id != null && !"".equals(id) ? id + "." : "") + quote(prop) + (jsonStyle ? ":" : "=" ) + toString(value);
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

    @Deprecated
    /**
     * use {@link Util#quote()}
     */
    public static String quote(String id) {
        return Util.quote(id);
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
        if (value instanceof Temporal){
            Value val = Values.of(value);
            return toStringFunction(val);
        }
        if (value instanceof DurationValue) {
            return toStringFunction((DurationValue) value);
        }
        return value.toString();
    }

    private static String toStringFunction(Value value) {
        return String.format(FUNCTION_TEMPLATE, value.getTypeName().toLowerCase(), value.toString());
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

    public static String cypherNode(Label label) {
        return String.format("(%s)", label == null ? "" : ":" + Util.quote(label.name()));
    }
    
    public static String simpleKeyValue(String key, Object value) {
        return String.format("{%s:%s}", key, value);
    }
}
