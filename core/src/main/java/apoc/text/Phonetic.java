package apoc.text;

import org.apache.commons.codec.language.DoubleMetaphone;
import org.neo4j.procedure.Description;
import org.apache.commons.codec.EncoderException;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.UserFunction;

import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.commons.codec.language.Soundex.US_ENGLISH;

public class Phonetic {

    private static final DoubleMetaphone DOUBLE_METAPHONE = new DoubleMetaphone();

    @UserFunction
    @Description("apoc.text.phonetic(text) yield value - Compute the US_ENGLISH phonetic soundex encoding of all words of the text")
    public String phonetic(final @Name("value") String value) {
        if (value == null) return null;
        return Stream.of(value.split("\\W+")).map(US_ENGLISH::soundex).collect(Collectors.joining(""));
    }

    @Procedure
    @Description("apoc.text.phoneticDelta(text1, text2) yield phonetic1, phonetic2, delta - Compute the US_ENGLISH soundex character difference between two given strings")
    public Stream<PhoneticResult> phoneticDelta(final @Name("text1") String text1, final @Name("text2") String text2) {
        try {
            return Stream.of(new PhoneticResult(US_ENGLISH.soundex(text1),US_ENGLISH.soundex(text2),US_ENGLISH.difference(text1,text2)));
        } catch (EncoderException e) {
            throw new RuntimeException("Error encoding text "+text1+" or "+text2+" for delta measure",e);
        }
    }

    @UserFunction
    @Description("apoc.text.doubleMetaphone(value) yield value - Compute the Double Metaphone phonetic encoding of all words of the text value")
    public String doubleMetaphone(final @Name("value") String value)
    {
        if (value == null || value.trim().isEmpty()) return value;
        return Stream.of(value.split("\\W+")).map(DOUBLE_METAPHONE::doubleMetaphone).collect(Collectors.joining(""));
    }

    public static class PhoneticResult {
        public final String phonetic1, phonetic2;
        public final long delta;

        public PhoneticResult(String phonetic1, String phonetic2, Number delta) {
            this.phonetic1 = phonetic1;
            this.phonetic2 = phonetic2;
            this.delta = delta.longValue();
        }
    }
}
