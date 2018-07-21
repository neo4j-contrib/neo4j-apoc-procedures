package apoc.export.csv;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class CsvHeaderFields {

    /**
     * Processes CSV header. Works for both nodes and relationships.
     *
     * @param header
     * @param delimiter
     * @param quotationCharacter
     * @return
     */
    public static List<CsvHeaderField> processHeader(final String header, final char delimiter, final char quotationCharacter) {
        final String separatorRegex = Pattern.quote(String.valueOf(delimiter));
        final List<String> attributes = Arrays.asList(header.split(separatorRegex));

        final List<CsvHeaderField> fieldEntries =
                IntStream.range(0, attributes.size())
                        .mapToObj(i -> CsvHeaderField.parse(i, attributes.get(i), quotationCharacter))
                        .collect(Collectors.toList());

        return fieldEntries;
    }

}
