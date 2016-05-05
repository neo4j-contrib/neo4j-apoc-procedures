package apoc.text;

import apoc.Description;
import apoc.result.BooleanResult;
import apoc.result.StringResult;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * @author mh
 * @since 05.05.16
 */
public class Strings {
    @Procedure
    @Description("apoc.text.replace(text, regex, replacement) YIELD value - replace each substring of the given string that matches the given regular expression with the given replacement.")
    public Stream<StringResult> replace(final @Name("text") String text, final @Name("regex") String regex, final @Name("replacement") String replacement) {
        if (text == null || regex == null || replacement == null) {
            return Stream.of(StringResult.EMPTY);
        }
        return Stream.of(new StringResult(text.replaceAll(regex, replacement)));
    }

    @Procedure
    @Description("apoc.text.clean(text) YIELD value - strip the given string of everything except alpha numeric characters and convert it to lower case.")
    public Stream<StringResult> clean(final @Name("text") String text) {
        return Stream.of(text == null ? StringResult.EMPTY : new StringResult(removeNonWordCharacters(text)));
    }

    @Procedure
    @Description("apoc.text.compareCleaned(text1, text2) YIELD value - compare the given strings stripped of everything except alpha numeric characters converted to lower case.")
    public Stream<BooleanResult> compareCleaned(final @Name("text1") String text1, final @Name("text2") String text2) {
        if (text1 == null || text2 == null) {
            return Stream.of(new BooleanResult(null));
        }
        return Stream.of(new BooleanResult((removeNonWordCharacters(text1).equals(removeNonWordCharacters(text2)))));
    }

    @Procedure
    @Description("apoc.text.filterCleanMatches(text1, text2) YIELD value - filter out non-matches of the given strings stripped of everything except alpha numeric characters converted to lower case.")
    public Stream<StringResult> filterCleanMatches(final @Name("text1") String text1, final @Name("text2") String text2) {

        return (text1 != null && text2 != null && removeNonWordCharacters(text1).equals(removeNonWordCharacters(text2))) ?
                Stream.of(new StringResult(text1)) :
                Stream.empty();
    }

    private static Pattern cleanPattern = Pattern.compile("[^A-Za-z0-9]+");

    private static String removeNonWordCharacters(String s) {
        return cleanPattern.matcher(s).replaceAll("").toLowerCase();
    }
}
