package apoc.number;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Locale;
import java.util.stream.Stream;

import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import org.neo4j.procedure.Description;
import apoc.result.DoubleResult;
import apoc.result.LongResult;
import apoc.result.StringResult;

/**
 * 
 * @since 25.8.2016
 * @author inserpio
 */
public class Number {

  // format
  
  @Procedure("apoc.number.format")
  @Description("apoc.number.format(number) yield value | format a long or double using the default system pattern and language to produce a string")
  public Stream<StringResult> format(final @Name("number") Object number) {
    return formatByPatternAndLanguage(number, null, null);
  }

  @Procedure("apoc.number.format.pattern")
  @Description("apoc.number.format.pattern(number, pattern) yield value | format a long or double using a pattern and the default system language to produce a string")
  public Stream<StringResult> formatByPattern(final @Name("number") Object number, final @Name("pattern") String pattern) {
    return formatByPatternAndLanguage(number, pattern, null);
  }
  
  @Procedure("apoc.number.format.lang")
  @Description("apoc.number.format.lang(number, lang) yield value | format a long or double using the default system pattern pattern and a language to produce a string")
  public Stream<StringResult> formatByLanguage(final @Name("number") Object number, final @Name("lang") String lang) {
    return formatByPatternAndLanguage(number, null, lang);
  }
  
  @Procedure("apoc.number.format.pattern.lang")
  @Description("apoc.number.format.pattern.lang(number, pattern, lang) yield value | format a long or double using a pattern and a language to produce a string")
  public Stream<StringResult> formatByPatternAndLanguage(final @Name("number") Object number, final @Name("pattern") String pattern, final @Name("lang") String lang) {
    validateNumberParam(number);
    DecimalFormat format = buildFormatter(pattern, lang);
    return Stream.of(new StringResult(format.format(number)));
  }
  
  // parseInt
  
  @Procedure("apoc.number.parseInt")
  @Description("apoc.number.parseInt(text) yield value | parse a text using the default system pattern and language to produce a long")
  public Stream<LongResult> parseInt(final @Name("text") String text) throws ParseException {
    return parseIntByPatternAndLanguage(text, null, null);
  }

  @Procedure("apoc.number.parseInt.pattern")
  @Description("apoc.number.parseInt.pattern(text, pattern) yield value | parse a text using a pattern and the default system language to produce a long")
  public Stream<LongResult> parseIntByPattern(final @Name("text") String text, @Name("pattern") String pattern) throws ParseException {
    return parseIntByPatternAndLanguage(text, pattern, null);
  }
  
  @Procedure("apoc.number.parseInt.lang")
  @Description("apoc.number.parseInt.lang(text, lang) yield value | parse a text using the default system pattern and a language to produce a long")
  public Stream<LongResult> parseIntByLanguage(final @Name("text") String text, @Name("lang") String lang) throws ParseException {
    return parseIntByPatternAndLanguage(text, null, lang);
  }
  
  @Procedure("apoc.number.parseInt.pattern.lang")
  @Description("apoc.number.parseInt.pattern.lang(text, pattern, lang) yield value | parse a text using a pattern and a language to produce a long")
  public Stream<LongResult> parseIntByPatternAndLanguage(final @Name("text") String text, @Name("pattern") String pattern, @Name("lang") String lang) throws ParseException {
    DecimalFormat format = buildFormatter(pattern, lang);
    return Stream.of(new LongResult(format.parse(text).longValue()));
  }
  
  // parseFloat
  
  @Procedure("apoc.number.parseFloat")
  @Description("apoc.number.parseFloat(text) yield value | parse a text using the default system pattern and language to produce a double")
  public Stream<DoubleResult> parseFloat(final @Name("text") String text) throws ParseException {
    return parseFloatByPatternAndLanguage(text, null, null);
  }
  
  @Procedure("apoc.number.parseFloat.pattern")
  @Description("apoc.number.parseFloat.pattern(text, pattern) yield value | parse a text using a pattern and the default system language to produce a double")
  public Stream<DoubleResult> parseFloatByPattern(final @Name("text") String text, @Name("pattern") String pattern) throws ParseException {
    return parseFloatByPatternAndLanguage(text, pattern, null);
  }
  
  @Procedure("apoc.number.parseFloat.lang")
  @Description("apoc.number.parseFloat.lang(text, lang) yield value | parse a text using the default system pattern and a language to produce a double")
  public Stream<DoubleResult> parseFloatByLanguage(final @Name("text") String text, @Name("lang") String lang) throws ParseException {
    return parseFloatByPatternAndLanguage(text, null, lang);
  }
  
  @Procedure("apoc.number.parseFloat.pattern.lang")
  @Description("apoc.number.parseFloat.pattern.lang(text, pattern, lang) yield value | parse a text using a pattern and a language to produce a double")
  public Stream<DoubleResult> parseFloatByPatternAndLanguage(final @Name("text") String text, @Name("pattern") String pattern, @Name("lang") String lang) throws ParseException {
    DecimalFormat format = buildFormatter(pattern, lang);
    return Stream.of(new DoubleResult(format.parse(text).doubleValue()));
  }

  private void validateNumberParam(Object number) {
    if (number == null) {
      throw new IllegalArgumentException("Number parameter mustn't be null.");
    }
    else if (!(number instanceof Long || number instanceof Double)) {
      throw new IllegalArgumentException("Number parameter must be long or double.");
    }
  }

  private DecimalFormat buildFormatter(String pattern, String language) {
    Locale locale = null;
    if (language != null) {
      if (!Arrays.asList(Locale.getISOLanguages()).contains(language)) {
        throw new IllegalArgumentException("Unrecognized language value: '" + language + "' isn't a valid ISO language");
      }
      locale = new Locale(language);
    }
    
    DecimalFormatSymbols symbols = null;
    if (locale != null) {
      symbols = new DecimalFormatSymbols(locale);
    }

    DecimalFormat format = null;
    if (pattern == null && symbols == null) {
      format = new DecimalFormat();
    }
    else if (pattern == null && symbols != null) {
      format = new DecimalFormat();
      format.setDecimalFormatSymbols(symbols);
    }
    else if (pattern != null && symbols == null) {
      format = new DecimalFormat(pattern);
    }
    else {
      format = new DecimalFormat(pattern, symbols);
    }
    
    return format;
  }
}
