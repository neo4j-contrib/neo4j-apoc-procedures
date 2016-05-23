package apoc.export.util;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;

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
}
