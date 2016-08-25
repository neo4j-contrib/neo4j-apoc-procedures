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

  @Procedure
  @Description("apoc.number.formatLong(number, pattern, locale) yield value | format a long using a (optional) pattern and for a (optional) locale to produce a string")
  public Stream<StringResult> formatLong(final @Name("number") long number, @Name("pattern") String pattern, @Name("language") String language) {
    DecimalFormat format = buildFormatter(pattern, language);
    return Stream.of(new StringResult(format.format(number)));
  }

  @Procedure
  @Description("apoc.number.formatDouble(number, pattern, locale) yield value | format a double using a (optional) pattern and for a (optional) locale to produce a string")
  public Stream<StringResult> formatDouble(final @Name("number") double number, @Name("pattern") String pattern, @Name("language") String language) {
    DecimalFormat format = buildFormatter(pattern, language);
    return Stream.of(new StringResult(format.format(number)));
  }
  
  @Procedure
  @Description("apoc.number.parseAsLong(text, pattern, locale) yield value | parse a text using a (optional) pattern and for a (optional) locale to produce a long")
  public Stream<LongResult> parseAsLong(final @Name("text") String text, @Name("pattern") String pattern, @Name("language") String language) throws ParseException {
    DecimalFormat format = buildFormatter(pattern, language);
    return Stream.of(new LongResult(format.parse(text).longValue()));
  }

  @Procedure
  @Description("apoc.number.parseAsDouble(text, pattern, locale) yield value | parse a text using a (optional) pattern and for a (optional) locale to produce a double")
  public Stream<DoubleResult> parseAsDouble(final @Name("text") String text, @Name("pattern") String pattern, @Name("language") String language) throws ParseException {
    DecimalFormat format = buildFormatter(pattern, language);
     return Stream.of(new DoubleResult(format.parse(text).doubleValue()));
  }

  @Procedure
  @Description("apoc.number.negativePrefix(pattern, locale) yield value | get the negative prefix for the given (optional) pattern and (optional) locale")
  public Stream<StringResult> negativePrefix(@Name("pattern") String pattern, @Name("language") String language) {
    DecimalFormat format = buildFormatter(pattern, language);
    return Stream.of(new StringResult(format.getNegativePrefix()));
  }
  
  @Procedure
  @Description("apoc.number.positivePrefix(pattern, locale) yield value | get the positive prefix for the given (optional) pattern and (optional) locale")
  public Stream<StringResult> positivePrefix(@Name("pattern") String pattern, @Name("language") String language) {
    DecimalFormat format = buildFormatter(pattern, language);
    return Stream.of(new StringResult(format.getPositivePrefix()));
  }

  @Procedure
  @Description("apoc.number.groupingSize(pattern, locale) yield value | get the grouping size for the given (optional) pattern and (optional) locale")
  public Stream<LongResult> groupingSize(@Name("pattern") String pattern, @Name("language") String language) {
    DecimalFormat format = buildFormatter(pattern, language);
    return Stream.of(new LongResult((long) format.getGroupingSize()));
  }

  @Procedure
  @Description("apoc.number.defaultNegativePrefix(pattern, locale) yield value | get the default negative prefix")
  public Stream<StringResult> defaultNegativePrefix() {
    DecimalFormat format = buildFormatter(null, null);
    return Stream.of(new StringResult(format.getNegativePrefix()));
  }
  
  @Procedure
  @Description("apoc.number.defaultPositivePrefix(pattern, locale) yield value | get the default positive prefix")
  public Stream<StringResult> defaultPositivePrefix() {
    DecimalFormat format = buildFormatter(null, null);
    return Stream.of(new StringResult(format.getPositivePrefix()));
  }
  
  @Procedure
  @Description("apoc.number.defaultGroupingSize(pattern, locale) yield value | get the default grouping size")
  public Stream<LongResult> defaultGroupingSize() {
    DecimalFormat format = buildFormatter(null, null);
    return Stream.of(new LongResult((long) format.getGroupingSize()));
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
