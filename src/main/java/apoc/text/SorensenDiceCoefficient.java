package apoc.text;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

public class SorensenDiceCoefficient {

  private static final double HIGHEST_SCORE = 1.0;

  public static double compute(String input1, String input2) {
    return compute(input1, input2, Locale.ENGLISH);
  }

  public static double compute(String input1, String input2, String languageTag) {
    return compute(input1, input2, Locale.forLanguageTag(languageTag));
  }

  private static double compute(String input1, String input2, Locale locale) {
    if (input1.equals(input2)) {
      return HIGHEST_SCORE;
    }

    List<String> words1 = normalizedWords(input1, locale);
    List<String> words2 = normalizedWords(input2, locale);
    if (words1.equals(words2)) {
      return HIGHEST_SCORE;
    }


    List<Bigram> bigrams1 = allBigrams(words1);
    List<Bigram> bigrams2 = allBigrams(words2);
    long count = bigrams2.stream()
        .filter(bigrams1::contains)
        .count();

    return 2.0 * count / (bigrams1.size() + bigrams2.size());
  }

  private static List<Bigram> allBigrams(List<String> words) {
    return words.stream()
                .flatMap(s -> toStream(s.toCharArray()))
                .collect(toList());
  }

  private static Stream<Bigram> toStream(char[] chars) {
    return IntStream.range(0, chars.length-1).mapToObj(i -> new Bigram(chars[i], chars[i+1]));
  }

  private static List<String> normalizedWords(String text1, Locale locale) {
    return Arrays.asList(text1.trim().toUpperCase(locale).split("\\s+"));
  }


  private static class Bigram {
    private final char first;
    private final char second;

    public Bigram(char first, char second) {
      this.first = first;
      this.second = second;
    }

    public char getFirst() {
      return first;
    }

    public char getSecond() {
      return second;
    }

    @Override public int hashCode() {
      return Objects.hash(first, second);
    }

    @Override public boolean equals(Object obj) {
      if (this == obj) {return true;}
      if (obj == null || getClass() != obj.getClass()) {return false;}
      final Bigram other = (Bigram) obj;
      return Objects.equals(this.first, other.first)
             && Objects.equals(this.second, other.second);
    }

    @Override public String toString() {
      return String.format("[%c,%c]", first, second);
    }
  }
}