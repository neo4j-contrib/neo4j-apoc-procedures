package apoc.export.util;

import apoc.util.JsonUtil;
import apoc.util.Util;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.spatial.Point;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static apoc.util.Util.labelStrings;
import static apoc.util.Util.map;

/**
 * @author mh
 * @since 23.02.16
 */
public class FormatUtils {

    // TODO - TODOISSIMO!! DOVREI ORDINARE LE LABEL E PROPS ALFABETICAMENTE!!!

    // todo - posso usare questo per gli id
    public static String formatNumber(Number value) {
        if (value == null) return null;
        return value.toString();
    }

    public static String formatString(Object value) {
        return "\"" + String.valueOf(value).replaceAll("\\\\", "\\\\\\\\")
                .replaceAll("\n","\\\\n")
                .replaceAll("\r","\\\\r")
                .replaceAll("\t","\\\\t")
                .replaceAll("\"","\\\\\"") + "\"";
    }

    public static String joinLabels(Node node, String delimiter) {
        return getLabelsAsStream(node).collect(Collectors.joining(delimiter));
    }

    /*
            private void writeNodeDetails(JsonGenerator jsonGenerator, Node node, boolean withNodeProperties) throws IOException {
            jsonGenerator.writeStringField("id", String.valueOf(node.getId()));

            if (node.getLabels().iterator().hasNext()) {
                jsonGenerator.writeArrayFieldStart("labels");

                List<String> labels = getLabelsSorted(node);
                for (String label : labels) {
                    jsonGenerator.writeString(label);
                }
                jsonGenerator.writeEndArray();
            }
            if (withNodeProperties) {
                serializeProperties(jsonGenerator, node.getAllProperties());
            }
        }
     */

    public static Map<String,Object> toMap(Entity pc, boolean isConvertToMap) {

        // todo - per le label SIA DI NODI START END CHE DEL NODO fare una cosa tipo il metodo commentato sopra

        if (pc == null) return null;
        if (pc instanceof Node) {
            Node node = (Node) pc;
            long id = node.getId();
            // todo - if properties is empty non lo metto se isConvertToMap.
            // todo - if more info è true metto metto anche type: node
            Map<String, Object> mapNode = Collections.emptyMap();

            Map<String, Object> props = pc.getAllProperties();
            // todo - forse basta map("properties", props) se isEmpty?
            if(isConvertToMap) {
                mapNode.putAll(map("id", String.valueOf(id), "type", "node"));
                if (!props.isEmpty()) {
                    mapNode.put("properties", props);
                }
                if (!node.getLabels().iterator().hasNext()) {
                    mapNode.put("labels", labelStrings(node));
                }
            } else {
                mapNode.putAll(map("id", id, "properties",props,"labels", labelStrings(node)));
            }

            return mapNode;
        }
        if (pc instanceof Relationship) {
            Relationship rel = (Relationship) pc;
            long id = rel.getId();

            // todo - rel.getType().name() che stampa??????
            //  nel caso cambiarlo o farlo con il ternario che ritorna 'relationship'
            String type = rel.getType().name();

            // todo - farlo come Node, cioè emptyMap e poi popolare
            Map<String, Object> mapRel = map("id", isConvertToMap ? String.valueOf(id) : id, "type", rel.getType().name());

            // todo - rel.getType().toString() e rel.getType().name() sono la stessa cosa??

            // todo - labels metterlo solo se ci sono labels nel risultato...

            // todo - forse basta map("properties", props) se isEmpty?
            Map<String, Object> props = pc.getAllProperties();
            if (isConvertToMap) {
                mapRel.putAll(map(/*"type", "relationship",*/
                        // todo - labels usando il metodo commentato di sopra
                        "start", map("id",String.valueOf(rel.getStartNode().getId()),
                                "labels", rel.getStartNode().getLabels()),
                        "end", map("id",String.valueOf(rel.getEndNode().getId()),
                                "labels", rel.getEndNode().getLabels())));

                // todo - forse non serve isEmpty??
                if (!props.isEmpty()) {
                    mapRel.put("properties", props);
                }
            } else {
                mapRel.putAll(map("start", rel.getStartNode().getId(),
                        "end", rel.getEndNode().getId(), "properties", props));
            }
            // todo - if properties is empty non lo metto se isConvertToMap.
            // todo - if more info è true metto in start ed end anche le labels
            // todo - if more info è true metto  anche type: relationship
            // todo - if more info è true metto anche le labels - jsonGenerator.writeStringField("label", rel.getType().toString());

            return mapRel;
//            return map("id", rel.getId(), "type", rel.getType().name(),
//                    "start", rel.getStartNode().getId(),"end", rel.getEndNode().getId(),
//                    "properties",pc.getAllProperties());
        }
        throw new RuntimeException("Invalid graph element "+pc);
    }

    public static Map<String,Object> toMap(Entity value) {
        return toMap(value, false);
    }
    public static String toString(Object value, boolean isConvertToMap) {
        // todo - path che fa?
        if (value == null) return "";
        if (value instanceof Path) {
            return toString(StreamSupport.stream(((Path)value).spliterator(),false).map(i->toMap(i, isConvertToMap)).collect(Collectors.toList()));
        }
        if (value instanceof Entity) {
            return Util.toJson(toMap((Entity) value, isConvertToMap), isConvertToMap); // todo id, label, type ?
        }
        if (value.getClass().isArray()) {
            if (isConvertToMap) {
                value = ((ArrayList) value).stream().map(i->toString(i, true)).collect(Collectors.toList());
            }

            return Util.toJson(value, isConvertToMap);
        }
        if (value instanceof Iterable) {
            if (isConvertToMap) {
                value = StreamSupport.stream(
                        ((Iterable) value).spliterator(), false)
                        .map(i->toString(i, true))
                        .collect(Collectors.toList());
            }
            return Util.toJson(value, isConvertToMap);
        }
        if (value instanceof Map) {
            if (isConvertToMap) {
                value = ((Map<String, Object>) value).entrySet()
                        .stream()
                        .collect(Collectors.toMap(Map.Entry::getKey,
                                e -> toString(e.getValue(), true)));
            }
            return Util.toJson(value, isConvertToMap);
        }
        if (value instanceof Number) {
            return formatNumber((Number)value);
        }
        if (value instanceof Point) {
            return formatPoint((Point) value);
        }
        return value.toString();
    }

    public static String toString(Object value) {
        return toString(value, false);
    }


    public static String formatPoint(Point value) {
        try {
            return JsonUtil.OBJECT_MAPPER.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public static List<String> getLabelsSorted(Node node) {
        return getLabelsAsStream(node).collect(Collectors.toList());
    }

    private static Stream<String> getLabelsAsStream(Node node) {
        return StreamSupport.stream(node.getLabels().spliterator(),false).map(Label::name).sorted();
    }
}
