package apoc.text;

import org.junit.Ignore;
import org.junit.Test;

import static org.hamcrest.number.IsCloseTo.closeTo;
import static org.junit.Assert.assertThat;

public class SorensenDiceCoefficientTest {

  @Test
  public void testSameStringsHaveHighestScore() {
    double score = SorensenDiceCoefficient.compute("hello", "hello", "en");

    assertThat(score, closeTo(1.0, 0.00001));
  }

  @Test
  public void testStringsWithOnlyDifferentCaseHaveHighestScore() {
    double score = SorensenDiceCoefficient.compute("HELLo", "heLlO", "en");

    assertThat(score, closeTo(1.0, 0.00001));
  }

  @Test
  public void testStringsWithOnlyDifferentSpacesHaveHighestScore() {
    double score = SorensenDiceCoefficient.compute("hello   world", " hello world", "en");

    assertThat(score, closeTo(1.0, 0.00001));
  }

  @Test
  public void testScoreIsProperlyComputed() {
    double score = SorensenDiceCoefficient.compute("quite similar", "quiet similaire", "en");

    int text1PairCount  = countOf("qu","ui","it","te")  + countOf("si","im","mi","il","la","ar");
    int text2PairCount  = countOf("qu","ui","ie","et")  + countOf("si","im","mi","il","la","ai","ir","re");
    int commonPairs     = countOf("qu","ui")            + countOf("si","im","mi","il","la");
    double expectedScore = 2.0 * commonPairs / (text1PairCount + text2PairCount);
    assertThat(score, closeTo((expectedScore), 0.00001));
  }

  @Test
  public void testScoreIsProperlyComputedWithCustomLanguageTag() {
    double score = SorensenDiceCoefficient.compute("çok ağrıyor", "az bilmiyor", "tr-TR");

    int text1PairCount  = countOf("ço","ok")  + countOf("ağ","ğr","rı","ıy",     "yo","or");
    int text2PairCount  = countOf("az")       + countOf("bi","il","lm","mi","iy","yo","or");
    int commonPairs     =                       countOf("yo","or");
    double expectedScore = 2.0 * commonPairs / (text1PairCount + text2PairCount);
    assertThat(score, closeTo((expectedScore), 0.00001));
  }

  @Test
  public void testScoreRepeatingCharactersCorrectly() {
    double score = SorensenDiceCoefficient.compute("aa", "aaaaaa");
    assertThat(score, closeTo(0.333333, 0.00001));

    score = SorensenDiceCoefficient.compute("aaaaaa", "aa");
    assertThat(score, closeTo(0.333333, 0.00001));
  }

  private int countOf(String... pairs) {
    return pairs.length;
  }
}