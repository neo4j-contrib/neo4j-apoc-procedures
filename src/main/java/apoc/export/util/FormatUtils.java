package apoc.export.util;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

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
        return "\"" + String.valueOf(value).replaceAll("\n","\\\\n").replaceAll("\t","\\\\t").replaceAll("\"","\\\\\"") + "\"";
    }

    public static String joinLabels(Node node, String delimiter) {
        return StreamSupport.stream(node.getLabels().spliterator(),false).map(Label::name).collect(Collectors.joining(delimiter));
    }

}
