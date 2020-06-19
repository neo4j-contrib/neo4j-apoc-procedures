package apoc.text;

import apoc.util.Util;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.similarity.HammingDistance;
import org.apache.commons.text.similarity.JaroWinklerDistance;
import org.apache.commons.text.similarity.LevenshteinDistance;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.text.Normalizer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static apoc.util.Util.quote;
import static java.lang.Math.toIntExact;
import static java.util.Arrays.asList;

/**
 * @author mh
 * @since 05.05.16
 */
public class Strings {

    private final static HammingDistance hammingDistance = new HammingDistance();
    private final static JaroWinklerDistance jaroWinklerDistance = new JaroWinklerDistance();
    private final static LevenshteinDistance levenshteinDistance = new LevenshteinDistance();

    @Context
    public Transaction tx;

    @UserFunction
    @Description("apoc.text.indexOf(text, lookup, from=0, to=-1==len) - find the first occurence of the lookup string in the text, from inclusive, to exclusive, -1 if not found, null if text is null.")
    public Long indexOf(final @Name("text") String text, final @Name("lookup") String lookup, final @Name(value = "from",defaultValue="0") long from, @Name(value = "to",defaultValue="-1") long to) {
        if (text==null) return null;
        if (lookup == null) return -1L;
        if (to == -1L || to > text.length()) return (long)text.indexOf(lookup,(int)from);
        if (to <= from) return -1L;
        return (long)text.substring(0,(int)to).indexOf(lookup,(int)from);
    }

    @UserFunction
    @Description("apoc.text.indexesOf(text, lookup, from=0, to=-1==len) - finds all occurences of the lookup string in the text, return list, from inclusive, to exclusive, empty list if not found, null if text is null.")
    public List<Long> indexesOf(final @Name("text") String text, final @Name("lookup") String lookup, final @Name(value = "from", defaultValue = "0") long from, @Name(value = "to", defaultValue = "-1") long to) {
        if (text == null) return null;
        if (lookup == null) return Collections.emptyList();
        if (to == -1L) to = text.length();
        List<Long> result = new ArrayList<>();
        int idx = (int) from - 1;
        while (true) {
            idx = text.indexOf(lookup, idx + 1);
            if (idx == -1 || idx >= to) {
                return result;
            } else {
                result.add((long) idx);
            }
        }
    }

    @UserFunction
    @Description("apoc.text.replace(text, regex, replacement) - replace each substring of the given string that matches the given regular expression with the given replacement.")
    public String replace(final @Name("text") String text, final @Name("regex") String regex, final @Name("replacement") String replacement) {
        return regreplace(text,regex,replacement);
    }

    @UserFunction
    @Description("apoc.text.byteCount(text,[charset]) - return size of text in bytes")
    public long byteCount(final @Name("text") String text, @Name(value = "charset", defaultValue = "UTF-8") String charset) throws UnsupportedEncodingException {
        return text.getBytes(charset).length;
    }
    @UserFunction
    @Description("apoc.text.bytes(text,[charset]) - return bytes of the text")
    public List<Long> bytes(final @Name("text") String text, @Name(value = "charset", defaultValue = "UTF-8") String charset) throws UnsupportedEncodingException {
        byte[] bytes = text.getBytes(charset);
        List<Long> result = new ArrayList<>(bytes.length);
        for (byte b : bytes) {
            result.add((long)b & 0xFFL);
        }
        return result;
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
    @Description("apoc.text.split(text, regex, limit) - splits the given text around matches of the given regex.")
    public List<String> split(final @Name("text") String text, final @Name("regex") String regex, final @Name(value = "limit", defaultValue = "0") Long limit) {
        if (text == null || regex == null || limit == null) {
            return null;
        }
        String[] resultArray = text.split(regex, limit.intValue());
        return new ArrayList<>(asList(resultArray));
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
    @Description("apoc.text.distance(text1, text2) - compare the given strings with the Levenshtein distance algorithm.")
    public Long distance(final @Name("text1") String text1, @Name("text2")final String text2) {
        return levenshteinDistance(text1, text2);
    }

    @UserFunction
    @Description("apoc.text.levenshteinDistance(text1, text2) - compare the given strings with the Levenshtein distance algorithm.")
    public Long levenshteinDistance(final @Name("text1") String text1, @Name("text2")final String text2) {
        if (text1 == null || text2 == null) {
            return null;
        }
        return (long)levenshteinDistance.apply(text1, text2);
    }

    @UserFunction
    @Description( "apoc.text.levenshteinSimilarity(text1, text2) - calculate the similarity (a value within 0 and 1) between two texts." )
    public Double levenshteinSimilarity(final @Name("text1") String text1, @Name("text2")final String text2) {
        if ( text1 == null || text2 == null ) {
            return null;
        }

        int longerLength = Math.max(text1.length(), text2.length());
        if (longerLength == 0) {
            return 1.0;
        }
        long editDistance = distance( text1, text2 );
        return (longerLength - editDistance) / (double)longerLength;
    }

    @UserFunction
    @Description( "apoc.text.hammingDistance(text1, text2) - compare the given strings with the Hamming distance algorithm." )
    public Long hammingDistance(final @Name("text1") String text1, @Name("text2")final String text2) {
        if (text1 == null || text2 == null) {
            return null;
        }
        return (long)hammingDistance.apply(text1, text2) ;
    }

    @UserFunction
    @Description( "apoc.text.jaroWinklerDistance(text1, text2) - compare the given strings with the Jaro-Winkler distance algorithm." )
    public Double jaroWinklerDistance(final @Name("text1") String text1, @Name("text2")final String text2) {
        if (text1 == null || text2 == null) {
            return null;
        }
        return jaroWinklerDistance.apply(text1, text2);
    }

    @UserFunction
    @Description("apoc.text.sorensenDiceSimilarityWithLanguage(text1, text2, languageTag) - compare the given strings with the Sørensen–Dice coefficient formula, with the provided IETF language tag")
    public Double sorensenDiceSimilarity(final @Name("text1") String text1, final @Name("text2") String text2, final @Name(value = "languageTag", defaultValue = "en") String languageTag) {
        if (text1 == null || text2 == null || languageTag == null) {
            return null;
        }
        return SorensenDiceCoefficient.compute(text1, text2, languageTag);
    }

    @UserFunction
    @Description("apoc.text.fuzzyMatch(text1, text2) - check if 2 words can be matched in a fuzzy way. Depending on the" +
                 " length of the String it will allow more characters that needs to be edited to match the second String.")
    public Boolean fuzzyMatch(final @Name("text1") String text1, @Name("text2")final String text2) {
        if (text1 == null || text2 == null) {
            return null;
        }
        int termLength = text1.length();
        int maxDistanceAllowed = termLength < 3 ? 0 : termLength < 5 ? 1 : 2;

        Long distance = distance(text1, text2);

        return distance <= maxDistanceAllowed;
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

    private static Pattern cleanPattern = Pattern.compile("[^\\p{L}\\p{N}]+");
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
    @Description("apoc.text.format(text,[params],language) - sprintf format the string with the params given")
    public String format(@Name("text") String text, @Name("params") List<Object> params, @Name(value = "language",defaultValue = "en") String lang) {
        if (text == null) return null;
        if (params == null) return text;
        return String.format(new Locale(lang),text, params.toArray());
    }

    @UserFunction
    @Description("apoc.text.slug(text, delim) - slug the text with the given delimiter")
    public String slug(@Name("text") String text, @Name(value = "delim", defaultValue = "-") String delim) {
        if (text == null) return null;
        if (delim == null) return null;
        return text.trim().replaceAll("[^\\p{L}0-9_]+", delim);
    }


    private static final String lower = "abcdefghijklmnopqrstuvwxyz";
    private static final String upper = lower.toUpperCase();
    private static final String numeric = "0123456789";

    @UserFunction
    @Description("apoc.text.random(length, valid) YIELD value - generate a random string")
    public String random(final @Name("length") long length, @Name(value = "valid", defaultValue = "A-Za-z0-9") String valid) {
        valid = valid.replaceAll("A-Z", upper).replaceAll("a-z", lower).replaceAll("0-9", numeric);

        StringBuilder output = new StringBuilder( toIntExact(length) );

        ThreadLocalRandom rand = ThreadLocalRandom.current();

        while ( output.length() < length ) {
            output.append( valid.charAt( rand.nextInt(valid.length()) ) );
        }

        return output.toString();
    }

    @UserFunction
    @Description("apoc.text.capitalize(text) YIELD value - capitalise the first letter of the word")
    public String capitalize(@Name("text") String text) {
        return text.substring(0, 1).toUpperCase() + text.substring(1);
    }

    @UserFunction
    @Description("apoc.text.capitalizeAll(text) YIELD value - capitalise the first letter of every word in the text")
    public String capitalizeAll(@Name("text") String text) {
        String[] parts = text.split(" ");

        StringBuilder output = new StringBuilder();

        for (String part : parts) {
            output.append( StringUtils.capitalize(part) + " " );

        }

        return output.toString().trim();
    }

    @UserFunction
    @Description("apoc.text.decapitalize(text) YIELD value - decapitalize the first letter of the word")
    public String decapitalize(@Name("text") String text) {
        return StringUtils.uncapitalize(text);
    }

    @UserFunction
    @Description("apoc.text.decapitalizeAll(text) YIELD value - decapitalize the first letter of all words")
    public String decapitalizeAll(@Name("text") String text) {
        String[] parts = text.split(" ");

        StringBuilder output = new StringBuilder();

        for (String part : parts) {
            output.append( StringUtils.uncapitalize(part) + " " );

        }

        return output.toString().trim();
    }

    @UserFunction
    @Description("apoc.text.swapCase(text) YIELD value - Swap the case of a string")
    public String swapCase(@Name("text") String text) {
        return StringUtils.swapCase(text);
    }

    @UserFunction
    @Description("apoc.text.camelCase(text) YIELD value - Convert a string to camelCase")
    public String camelCase(@Name("text") String text) {
        text = text.replaceAll("[^\\p{L}0-9]|_", " ");

        String[] parts = text.split("(\\s+)");
        StringBuilder output = new StringBuilder();

        for (String part : parts) {
            part = part.toLowerCase();

            output.append( StringUtils.capitalize( part ) );
        }

        return output.substring(0, 1).toLowerCase() + output.substring(1);
    }

    @UserFunction
    @Description("apoc.text.upperCamelCase(text) YIELD value - Convert a string to camelCase")
    public String upperCamelCase(@Name("text") String text) {
        String output = camelCase(text);

        return output.substring(0, 1).toUpperCase() + output.substring(1);
    }

    @UserFunction
    @Description("apoc.text.snakeCase(text) YIELD value - Convert a string to snake-case")
    public String snakeCase(@Name("text") String text) {
        // Convert Snake Case
        if ( text.matches("^([\\p{Lu}0-9_]+)$") ) {
            text = text.toLowerCase().replace("_", " ");
        }

        String[] parts = text.split("(?=[^\\p{Ll}0-9])");
        StringBuilder output = new StringBuilder();

        for (String part : parts) {
            part = part.trim();

            if (part.length() > 0) {
                if (output.length() > 0) {
                    output.append("-");
                }

                output.append( part.toLowerCase().trim().replace("(^[\\p{Ll}0-9]+)", "-") );
            }
        }

        return output.toString().toLowerCase().replaceAll("--", "-");
    }

    @UserFunction
    @Description("apoc.text.toUpperCase(text) YIELD value - Convert a string to UPPER_CASE")
    public String toUpperCase(@Name("text") String text) {
        String[] parts = text.split("(?=[^a-z0-9]+)");
        StringBuilder output = new StringBuilder();

        for (String part : parts) {
            part = part.trim().toUpperCase().replaceAll("[^A-Z0-9]+", "");

            if (part.length() > 0) {
                if (output.length() > 0) {
                    output.append("_");
                }

                output.append(part);
            }
        }

        return output.toString();
    }

    @UserFunction
    @Description("apoc.text.base64Encode(text) YIELD value - Encode a string with Base64")
    public String base64Encode(@Name("text") String text) {
        byte[] encoded = Base64.getEncoder().encode(text.getBytes());
        return new String(encoded);
    }

    @UserFunction
    @Description("apoc.text.base64Decode(text) YIELD value - Decode Base64 encoded string")
    public String base64Decode(@Name("text") String text) {
        byte[] decoded = Base64.getDecoder().decode(text.getBytes());
        return new String(decoded);
    }

    @UserFunction
    @Description("apoc.text.base64UrlEncode(text) YIELD value - Encode a url with Base64")
    public String base64UrlEncode(@Name("url") String url) {
        byte[] encoded = Base64.getUrlEncoder().encode(url.getBytes());
        return new String(encoded);
    }

    @UserFunction
    @Description("apoc.text.base64UrlDecode(url) YIELD value - Decode Base64 encoded url")
    public String base64UrlDecode(@Name("url") String url) {
        byte[] decoded = Base64.getUrlDecoder().decode(url.getBytes());
        return new String(decoded);
    }

    @UserFunction
    @Description("apoc.text.charAt(text, index) - the decimal value of the character at the given index")
    public Long charAt(@Name("text") String text, @Name("index") Long index) {
        if (index == null || text == null || text.isEmpty() || index < 0 || index >= text.length()) {
            return null;
        }
        return ((long) text.charAt(index.intValue()));
    }

    @UserFunction
    @Description("apoc.text.code(codepoint) - Returns the unicode character of the given codepoint")
    public String code(@Name("codepoint") Long codepoint) {
        if (codepoint == null || codepoint < 0 || codepoint > Character.MAX_VALUE) {
            return null;
        }
        return String.valueOf((char)codepoint.intValue());
    }

    @UserFunction
    @Description("apoc.text.hexValue(value) - the hex value string of the given number")
    public String hexValue(@Name("value") Long value) {
        if (value == null) {
            return null;
        }
        return value > 0xFFFFFFFFL ? String.format("%016X", value) : value > 0xFFFFL ?  String.format("%08X", (int)value.intValue()) : String.format("%04X", (int)value.intValue());
    }

    @UserFunction
    @Description("apoc.text.hexCharAt(text, index) - the hex value string of the character at the given index")
    public String hexCharAt(@Name("text") String text, @Name("index") Long index) {
        return hexValue(charAt(text, index));
    }

    private boolean isPrimitive(Object value) {
        return value == null || value instanceof String || value instanceof Number || value instanceof Boolean;
    }

    private String cypherName(Map<String,Object> config, String key, Supplier<String> s, Function<String,String> quoter) {
        Object name = config.get(key);
        if (name!=null) return quoter.apply(name.toString());
        return s.get();
    }

    @UserFunction
    @Description("apoc.text.toCypher(value, {skipKeys,keepKeys,skipValues,keepValues,skipNull,node,relationship,start,end}) | tries it's best to convert the value to a cypher-property-string")
    public String toCypher(@Name("value") Object value, @Name(value = "config",defaultValue = "{}") Map<String,Object> config) {
        if (config.containsKey("keepValues") && !((Collection)config.get("keepValues")).stream().noneMatch((v) -> (v.getClass().isInstance(value) || isPrimitive(value) && isPrimitive(v)) && !value.equals(v))) return null;
        else if (config.containsKey("skipValues") && ((Collection) config.get("skipValues")).contains(value)) return null;

        if (value==null) return "null";
        if (value instanceof Number || value instanceof Boolean) return value.toString();
        if (value instanceof String) return '\''+value.toString()+'\'';
        if (value instanceof Iterable) return '['+StreamSupport.stream(((Iterable<?>)value).spliterator(),false).map(v -> toCypher(v,config)).filter(Objects::nonNull).collect(Collectors.joining(","))+']';
        if (value.getClass().isArray()) return '['+Arrays.stream((Object[])value).map(v -> toCypher(v,config)).filter(Objects::nonNull).collect(Collectors.joining(","))+']';
        if (value instanceof Node) {
            Node node = Util.rebind(tx, (Node)value);
            String labels = StreamSupport.stream(node.getLabels().spliterator(),false).map(l -> quote(l.name())).collect(Collectors.joining(":"));
            if (!labels.isEmpty()) labels = ':'+labels;
            String var = cypherName(config, "node", () -> "", Util::quote);
            return '('+ var +labels+' '+ toCypher(node.getAllProperties(), config)+')';
        }
        if (value instanceof Relationship) {
            Relationship rel = Util.rebind(tx, (Relationship) value);
            String type = ':'+quote(rel.getType().name());
            String start = cypherName(config, "start", () -> toCypher(rel.getStartNode(), config),(s)->'('+quote(s)+')');
            String relationship = cypherName(config, "relationship", () -> "", Util::quote);
            String end = cypherName(config,"end", ()->toCypher(rel.getEndNode(),config),(s)->'('+quote(s)+')');
            return start+"-["+relationship+type+' '+ toCypher(rel.getAllProperties(), config)+"]->"+ end;
        }
        if (value instanceof Map) {
            Map<String,Object> values = (Map<String, Object>) value;
            if (config.containsKey("keepKeys")) {
                values.keySet().retainAll((List<String>)(config.get("keepKeys")));
            }
            if (config.containsKey("skipKeys")) {
                values.keySet().removeAll((List<String>)(config.get("skipKeys")));
            }
            return '{'+values.entrySet().stream()
                    .map((e)-> Pair.of(e.getKey(), toCypher(e.getValue(),config)))
                    .filter((p)->p.other() != null)
                    .sorted(Comparator.comparing(Pair::first))
                    .map((p) -> quote(p.first())+":"+p.other())
                    .collect(Collectors.joining(","))+'}';
        }
        return null;
    }

    @UserFunction
    @Description("apoc.text.repeat(item, count) - string multiplication")
    public String repeat(@Name("item") String item, @Name("count") long count) {
        StringBuilder result = new StringBuilder((int) count * item.length());
        for (int i=0; i<count; i++) {
            result.append(item);
        }
        return result.toString();
    }
}
