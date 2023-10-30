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

import apoc.util.TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import static apoc.util.TestUtil.testCall;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

public class PhoneticTest {

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();


    @BeforeClass
    public static void setUp() {
        TestUtil.registerProcedure(db, Phonetic.class);
    }

    @AfterClass
    public static void teardown() {
       db.shutdown();
    }

    @Test
    public void shouldComputeSimpleSoundexEncoding() {
        testCall(db, "CALL apoc.text.phonetic('HellodearUser!')", (row) ->
            assertThat(row.get("value"), equalTo("H436"))
        );
        testCall(db, "RETURN apoc.text.phonetic('HellodearUser!') as value", (row) ->
            assertThat(row.get("value"), equalTo("H436"))
        );
    }

    @Test
    public void shouldComputeSimpleSoundexEncodingOfNull() {
        testCall(db, "CALL apoc.text.phonetic(null)", (row) ->
                assertThat(row.get("value"), equalTo(null))
        );
        testCall(db, "RETURN apoc.text.phonetic(null) as value", (row) ->
                assertThat(row.get("value"), equalTo(null))
        );
    }

    @Test
    public void shouldComputeEmptySoundexEncodingForTheEmptyString() {
        testCall(db, "CALL apoc.text.phonetic('')", (row) ->
                assertThat(row.get("value"), equalTo(""))
        );
        testCall(db, "RETURN apoc.text.phonetic('') as value", (row) ->
                assertThat(row.get("value"), equalTo(""))
        );
    }

    @Test
    public void shouldComputeSoundexEncodingOfManyWords() {
        testCall(db, "CALL apoc.text.phonetic('Hello, dear User!')", (row) ->
                assertThat(row.get("value"), equalTo("H400D600U260"))
        );
        testCall(db, "RETURN apoc.text.phonetic('Hello, dear User!') as value", (row) ->
                assertThat(row.get("value"), equalTo("H400D600U260"))
        );
    }


    @Test
    public void shouldComputeSoundexEncodingOfManyWordsEvenIfTheStringContainsSomeExtraChars() {
        testCall(db, "CALL apoc.text.phonetic('  ,Hello,  dear User 5!')", (row) ->
                assertThat(row.get("value"), equalTo("H400D600U260"))
        );
        testCall(db, "RETURN apoc.text.phonetic('  ,Hello,  dear User 5!') as value", (row) ->
                assertThat(row.get("value"), equalTo("H400D600U260"))
        );
    }

    @Test
    public void shouldComputeSoundexDifference() {
        testCall(db, "CALL apoc.text.phoneticDelta('Hello Mr Rabbit', 'Hello Mr Ribbit')", (row) ->
                assertThat(row.get("delta"), equalTo(4L))
        );
    }

    @Test
    public void shoudlComputeDoubleMetaphone() {
        testCall(db, "CALL apoc.text.doubleMetaphone('Apoc')", (row) ->
                assertThat(row.get("value"), equalTo("APK"))
        );
        testCall(db, "RETURN apoc.text.doubleMetaphone('Apoc') as value", (row) ->
                assertThat(row.get("value"), equalTo("APK"))
        );
    }

    @Test
    public void shoudlComputeDoubleMetaphoneOfNull() {
        testCall(db, "CALL apoc.text.doubleMetaphone(NULL)", (row) ->
                assertThat(row.get("value"), equalTo(null))
        );
        testCall(db, "RETURN apoc.text.doubleMetaphone(NULL) as value", (row) ->
                assertThat(row.get("value"), equalTo(null))
        );
    }

    @Test
    public void shoudlComputeDoubleMetaphoneForTheEmptyString() {
        testCall(db, "CALL apoc.text.doubleMetaphone('')", (row) ->
                assertThat(row.get("value"), equalTo(""))
        );
        testCall(db, "RETURN apoc.text.doubleMetaphone('') as value", (row) ->
                assertThat(row.get("value"), equalTo(""))
        );
    }

    @Test
    public void shouldComputeDoubleMetaphoneOfManyWords() {
        testCall(db, "CALL apoc.text.doubleMetaphone('Hello, dear User!')", (row) ->
                assertThat(row.get("value"), equalTo("HLTRASR"))
        );
        testCall(db, "RETURN apoc.text.doubleMetaphone('Hello, dear User!') as value    ", (row) ->
                assertThat(row.get("value"), equalTo("HLTRASR"))
        );
    }
}
