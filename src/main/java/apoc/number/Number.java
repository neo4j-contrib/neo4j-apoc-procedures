package apoc.number;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Locale;
import java.util.stream.Stream;

import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import apoc.Description;
import apoc.result.DoubleResult;
import apoc.result.LongResult;
import apoc.result.StringResult;

/**
 * 
 * @since 25.8.2016
 * @author inserpio
 */
public class Number {

  // Format Long or Double
  
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
  
  // Parse Long
  
  @Procedure("apoc.number.parseLong")
  @Description("apoc.number.parseLong(text) yield value | parse a text using the default system pattern and language to produce a long")
  public Stream<LongResult> parseLong(final @Name("text") String text) throws ParseException {
    return parseLongByPatternAndLanguage(text, null, null);
  }

  @Procedure("apoc.number.parseLong.pattern")
  @Description("apoc.number.parseLong.pattern(text, pattern) yield value | parse a text using a pattern and the default system language to produce a long")
  public Stream<LongResult> parseLongByPattern(final @Name("text") String text, @Name("pattern") String pattern) throws ParseException {
    return parseLongByPatternAndLanguage(text, pattern, null);
  }
  
  @Procedure("apoc.number.parseLong.lang")
  @Description("apoc.number.parseLong.lang(text, lang) yield value | parse a text using the default system pattern and a language to produce a long")
  public Stream<LongResult> parseLongByLanguage(final @Name("text") String text, @Name("lang") String lang) throws ParseException {
    return parseLongByPatternAndLanguage(text, null, lang);
  }
  
  @Procedure("apoc.number.parseLong.pattern.lang")
  @Description("apoc.number.parseLong.pattern.lang(text, pattern, lang) yield value | parse a text using a pattern and a language to produce a long")
  public Stream<LongResult> parseLongByPatternAndLanguage(final @Name("text") String text, @Name("pattern") String pattern, @Name("lang") String lang) throws ParseException {
    DecimalFormat format = buildFormatter(pattern, lang);
    return Stream.of(new LongResult(format.parse(text).longValue()));
  }
  
  // Parse Double
  
  @Procedure("apoc.number.parseDouble")
  @Description("apoc.number.parseDouble(text) yield value | parse a text using the default system pattern and language to produce a double")
  public Stream<DoubleResult> parseDouble(final @Name("text") String text) throws ParseException {
    return parseDoubleByPatternAndLanguage(text, null, null);
  }
  
  @Procedure("apoc.number.parseDouble.pattern")
  @Description("apoc.number.parseDouble.pattern(text, pattern) yield value | parse a text using a pattern and the default system language to produce a double")
  public Stream<DoubleResult> parseDoubleByPattern(final @Name("text") String text, @Name("pattern") String pattern) throws ParseException {
    return parseDoubleByPatternAndLanguage(text, pattern, null);
  }
  
  @Procedure("apoc.number.parseDouble.lang")
  @Description("apoc.number.parseDouble.lang(text, lang) yield value | parse a text using the default system pattern and a language to produce a double")
  public Stream<DoubleResult> parseDoubleByLanguage(final @Name("text") String text, @Name("lang") String lang) throws ParseException {
    return parseDoubleByPatternAndLanguage(text, null, lang);
  }
  
  @Procedure("apoc.number.parseDouble.pattern.lang")
  @Description("apoc.number.parseDouble.pattern.lang(text, pattern, lang) yield value | parse a text using a pattern and a language to produce a double")
  public Stream<DoubleResult> parseDoubleByPatternAndLanguage(final @Name("text") String text, @Name("pattern") String pattern, @Name("lang") String lang) throws ParseException {
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
