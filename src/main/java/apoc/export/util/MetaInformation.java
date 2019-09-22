package apoc.export.util;

import apoc.meta.Meta;
import org.neo4j.cypher.export.SubGraph;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import static apoc.gephi.GephiFormatUtils.getCaption;
import static java.util.Arrays.asList;

/**
 * @author mh
 * @since 19.01.14
 */
public class MetaInformation {

    public static Map<String,Class> collectPropTypesForNodes(SubGraph graph) {
        Map<String,Class> propTypes = new LinkedHashMap<>();
        for (Node node : graph.getNodes()) {
            updateKeyTypes(propTypes, node);
        }
        return propTypes;
    }
    public static Map<String,Class> collectPropTypesForRelationships(SubGraph graph) {
        Map<String,Class> propTypes = new LinkedHashMap<>();
        for (Relationship node : graph.getRelationships()) {
            updateKeyTypes(propTypes, node);
        }
        return propTypes;
    }

    public static void updateKeyTypes(Map<String, Class> keyTypes, Entity pc) {
        for (String prop : pc.getPropertyKeys()) {
            Object value = pc.getProperty(prop);
            Class storedClass = keyTypes.get(prop);
            if (storedClass==null) {
                keyTypes.put(prop,value.getClass());
                continue;
            }
            if (storedClass == void.class || storedClass.equals(value.getClass())) continue;
            keyTypes.put(prop, void.class);
        }
    }

    public final static Set<String> GRAPHML_ALLOWED = new HashSet<>(asList("boolean", "int", "long", "float", "double", "string"));

    public static String typeFor(Class value, Set<String> allowed) {
        if (value == void.class) return null; // Is this necessary?
        Meta.Types type = Meta.Types.of(value);
        String name = (value.isArray() ? value.getComponentType() : value).getSimpleName().toLowerCase();
        boolean isAllowed = allowed != null && allowed.contains(name);
        switch (type) {
            case NULL:
                return null;
            case INTEGER: case FLOAT:
                return "integer".equals(name) || !isAllowed ? "int" : name;
            default:
                return isAllowed ? name : "string"; // We manage all other data types as strings
        }
    }

    public static String getLabelsString(Node node) {
        if (!node.getLabels().iterator().hasNext()) return "";
        String delimiter = ":";
        return delimiter + FormatUtils.joinLabels(node, delimiter);
    }

    public static String getLabelsStringGephi(ExportConfig config, Node node) {
        return getCaption(node, config.getCaption());
    }
}
