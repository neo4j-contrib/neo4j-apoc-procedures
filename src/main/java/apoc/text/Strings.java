package apoc.text;

import apoc.Description;
import apoc.result.BooleanResult;
import apoc.result.Empty;
import apoc.result.StringResult;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.text.Normalizer;
import java.util.List;
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
    @Description("apoc.text.join(['text1','text2',...], delimiter) YIELD value - join the given strings with the given delimiter.")
    public Stream<StringResult> join(
            final @Name("texts") List<String> texts,
            final @Name("delimiter") String delimiter) {
        if (texts == null || delimiter == null) {
            return Stream.of(StringResult.EMPTY);
        }
        return Stream.of(new StringResult(String.join(delimiter, texts)));
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
    @Description("apoc.text.filterCleanMatches(text1, text2) - filter out non-matches of the given strings stripped of everything except alpha numeric characters converted to lower case.")
    public Stream<Empty> filterCleanMatches(final @Name("text1") String text1, final @Name("text2") String text2) {

        boolean matched = text1 != null && text2 != null && removeNonWordCharacters(text1).equals(removeNonWordCharacters(text2));
        return Empty.stream(matched);
    }

    @Procedure
    @Description("apoc.text.urlencode(text) - return the urlencoded text")
    public Stream<StringResult> urlencode(@Name("text") String text) {
        try {
            return Stream.of(new StringResult(URLEncoder.encode(text, "UTF-8")));
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("urlencoding failed", e);
        }
    }

    @Procedure
    @Description("apoc.text.urldecode(text) - return the urldecoded text")
    public Stream<StringResult> urldecode(@Name("text") String text) {
        try {
            return Stream.of(new StringResult(URLDecoder.decode(text, "UTF-8")));
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("urldecoding failed", e);
        }
    }

    private static Pattern cleanPattern = Pattern.compile("[^A-Za-z0-9]+");
    private static Pattern specialCharPattern = Pattern.compile("\\p{IsM}+");
    private static String[][] UMLAUT_REPLACEMENTS = {
            { new String("Ä"), "Ae" },
            { new String("Ü"), "Ue" },
            { new String("Ö"), "Oe" },
            { new String("ä"), "ae" },
            { new String("ü"), "ue" },
            { new String("ö"), "oe" },
            { new String("ß"), "ss" }
    };


    private static String removeNonWordCharacters(String s) {

        String result = s ;
        for (int i=0; i<UMLAUT_REPLACEMENTS.length; i++) {
            result = result.replace(UMLAUT_REPLACEMENTS[i][0], UMLAUT_REPLACEMENTS[i][1]);
        }
        result = Normalizer.normalize(result, Normalizer.Form.NFD);
        String tmp2 = specialCharPattern.matcher(result).replaceAll("");
        return cleanPattern.matcher(tmp2).replaceAll("").toLowerCase();
    }
}
