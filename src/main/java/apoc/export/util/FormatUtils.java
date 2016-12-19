package apoc.export.util;

import apoc.util.Util;
import org.neo4j.graphdb.*;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static apoc.util.Util.labelStrings;
import static apoc.util.Util.map;

/**
 * @author mh
 * @since 23.02.16
 */
public class FormatUtils {

    public static DecimalFormat decimalFormat = new DecimalFormat() {
        {
            setMaximumFractionDigits(340);
            setGroupingUsed(false);
            setDecimalSeparatorAlwaysShown(false);
            DecimalFormatSymbols symbols = getDecimalFormatSymbols();
            symbols.setDecimalSeparator('.');
            setDecimalFormatSymbols(symbols);
        }
    };

    public static String formatNumber(Number value) {
        if (value == null) return null;
        return decimalFormat.format(value);
    }

    public static String formatString(Object value) {
        return "\"" + String.valueOf(value).replaceAll("\\\\", "\\\\\\\\").replaceAll("\n","\\\\n").replaceAll("\t","\\\\t").replaceAll("\"","\\\\\"") + "\"";
    }

    public static String joinLabels(Node node, String delimiter) {
        return StreamSupport.stream(node.getLabels().spliterator(),false).map(Label::name).collect(Collectors.joining(delimiter));
    }

    public static Map<String,Object> toMap(PropertyContainer pc) {
        if (pc == null) return null;
        if (pc instanceof Node) {
            Node node = (Node) pc;
            return map("id", node.getId(), "labels",labelStrings(node),
                    "properties",pc.getAllProperties());
        }
        if (pc instanceof Relationship) {
            Relationship rel = (Relationship) pc;
            return map("id", rel.getId(), "type", rel.getType().name(),
                    "start", rel.getStartNode().getId(),"end", rel.getEndNode().getId(),
                    "properties",pc.getAllProperties());
        }
        throw new RuntimeException("Invalid graph element "+pc);
    }
    public static String toString(Object value) {
        if (value == null) return "";
        if (value instanceof Path) {
            return toString(StreamSupport.stream(((Path)value).spliterator(),false).map(FormatUtils::toMap).collect(Collectors.toList()));
        }
        if (value instanceof PropertyContainer) {
            return Util.toJson(toMap((PropertyContainer) value)); // todo id, label, type ?
        }
        if (value.getClass().isArray() || value instanceof Iterable || value instanceof Map) {
            return Util.toJson(value);
        }
        if (value instanceof Number) {
            return formatNumber((Number)value);
        }
        return value.toString();
    }
}
