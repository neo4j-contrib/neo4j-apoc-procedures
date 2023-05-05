/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package apoc.text;

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