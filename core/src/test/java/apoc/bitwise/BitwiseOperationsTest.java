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
package apoc.bitwise;

import apoc.util.TestUtil;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.Map;

import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testCall;
import static org.junit.Assert.assertEquals;

public class BitwiseOperationsTest {

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    public static final String BITWISE_CALL = "return apoc.bitwise.op($a,$op,$b) as value";

    private int a,b;

    @BeforeClass
    public static void setUp() throws Exception {
        TestUtil.registerProcedure(db, BitwiseOperations.class);
    }

    public void testOperation(String op, long expected) {
        Map<String, Object> params = map("a", a, "op", op, "b", b);
        testCall(db, BITWISE_CALL, params,
                (row) -> assertEquals("operation " + op, expected, row.get("value")));
    }

    @Test
    public void testOperations() throws Throwable {
        a = 0b0011_1100;
        b = 0b0000_1101;
        testOperation("&", 12L);
        testOperation("AND", 12L);
        testOperation("OR", 61L);
        testOperation("|", 61L);
        testOperation("^", 49L);
        testOperation("XOR", 49L);
        testOperation("~", -61L);
        testOperation("NOT", -61L);
    }

    @Test
    public void testOperations2() throws Throwable {
        a = 0b0011_1100;
        b = 2;
        testOperation("<<", 240L);
        testOperation("left shift", 240L);
        testOperation(">>", 15L);
        testOperation("right shift", 15L);
        testOperation("right shift unsigned", 15L);
        testOperation(">>>", 15L);
    }
}
