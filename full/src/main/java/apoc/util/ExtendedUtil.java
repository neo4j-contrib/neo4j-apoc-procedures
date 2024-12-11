package apoc.util;

import java.util.Collection;
import java.util.stream.Collectors;
import org.apache.commons.collections4.CollectionUtils;

public class ExtendedUtil {
    public static String joinStringLabels(Collection<String> labels) {
        return CollectionUtils.isNotEmpty(labels)
                ? ":" + labels.stream().map(Util::quote).collect(Collectors.joining(":"))
                : "";
    }
}
