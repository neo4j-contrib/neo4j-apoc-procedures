package apoc.text;

import org.neo4j.procedure.Description;
import apoc.result.StringResult;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.UserFunction;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.text.Normalizer;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * @author mh
 * @since 05.05.16
 */
public class Strings {

    @UserFunction
    @Description("apoc.text.replace(text, regex, replacement) - replace each substring of the given string that matches the given regular expression with the given replacement.")
    public String replace(final @Name("text") String text, final @Name("regex") String regex, final @Name("replacement") String replacement) {
        return regreplace(text,regex,replacement);
    }
    @UserFunction
    @Description("apoc.text.regreplace(text, regex, replacement) - replace each substring of the given string that matches the given regular expression with the given replacement.")
    public String regreplace(final @Name("text") String text, final @Name("regex") String regex, final @Name("replacement") String replacement) {
        if (text == null || regex == null || replacement == null) {
            return null;
        }
        return text.replaceAll(regex, replacement);
    }

    @UserFunction
    @Description("apoc.text.regexGroups(text, regex) - return all matching groups of the regex on the given text.")
    public List<List<String>> regexGroups(final @Name("text") String text, final @Name("regex") String regex) {
        if (text==null || regex==null) {
            return Collections.EMPTY_LIST;
        } else {
            final Pattern pattern = Pattern.compile(regex);
            final Matcher matcher = pattern.matcher(text);

            List<List<String>> result = new ArrayList<>();
            while (matcher.find()) {
                List<String> matchResult = new ArrayList<>();
                for (int i=0;i<=matcher.groupCount(); i++) {
                    matchResult.add(matcher.group(i));
                }
                result.add(matchResult);
            }
            return result;
        }
    }


    @UserFunction
    @Description("apoc.text.join(['text1','text2',...], delimiter) - join the given strings with the given delimiter.")
    public String join(
            final @Name("texts") List<String> texts,
            final @Name("delimiter") String delimiter) {
        if (texts == null || delimiter == null) {
            return null;
        }
        return String.join(delimiter, texts);
    }

    @UserFunction
    @Description("apoc.text.clean(text) - strip the given string of everything except alpha numeric characters and convert it to lower case.")
    public String clean(final @Name("text") String text) {
        return text == null ? null : removeNonWordCharacters(text);
    }

    @UserFunction
    @Description("apoc.text.compareCleaned(text1, text2) - compare the given strings stripped of everything except alpha numeric characters converted to lower case.")
    public boolean compareCleaned(final @Name("text1") String text1, final @Name("text2") String text2) {
        if (text1 == null || text2 == null) {
            return false;
        }
        return removeNonWordCharacters(text1).equals(removeNonWordCharacters(text2));
    }

    @UserFunction
    @Description("apoc.text.urlencode(text) - return the urlencoded text")
    public String urlencode(@Name("text") String text) {
        try {
            return URLEncoder.encode(text, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("urlencoding failed", e);
        }
    }

    @UserFunction
    @Description("apoc.text.urldecode(text) - return the urldecoded text")
    public String urldecode(@Name("text") String text) {
        try {
            return URLDecoder.decode(text, "UTF-8");
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


    @UserFunction
    @Description("apoc.text.lpad(text,count,delim) YIELD value - left pad the string to the given width")
    public String lpad(@Name("text") String text, @Name("count") long count,@Name(value = "delim",defaultValue = " ") String delim) {
        int len = text.length();
        if (len >= count) return text;
        StringBuilder sb = new StringBuilder((int)count);
        char[] chars = new char[(int)count - len];
        Arrays.fill(chars, delim.charAt(0));
        sb.append(chars);
        sb.append(text);
        return sb.toString();
    }

    @UserFunction
    @Description("apoc.text.rpad(text,count,delim) YIELD value - right pad the string to the given width")
    public String rpad(@Name("text") String text, @Name("count") long count, @Name(value = "delim",defaultValue = " ") String delim) {
        int len = text.length();
        if (len >= count) return text;
        StringBuilder sb = new StringBuilder(text);
        char[] chars = new char[(int)count - len];
        Arrays.fill(chars, delim.charAt(0));
        sb.append(chars);
        return sb.toString();
    }

    @UserFunction
    @Description("apoc.text.format(text,[params]) - sprintf format the string with the params given")
    public String format(@Name("text") String text, @Name("params") List<Object> params) {
        if (text == null) return null;
        if (params == null) return text;
        return String.format(Locale.ENGLISH,text, params.toArray());
    }
}
