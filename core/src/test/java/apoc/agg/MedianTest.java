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
package apoc.agg;

import apoc.util.TestUtil;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import static apoc.util.TestUtil.testCall;
import static org.junit.Assert.assertEquals;

public class MedianTest {

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    @BeforeClass public static void setUp() throws Exception {
        TestUtil.registerProcedure(db, Median.class);
    }

    @Test
    public void testMedian() throws Exception {
        testCall(db, "UNWIND [] as value RETURN apoc.agg.median(value) as p",
                (row) -> {
                    assertEquals(null, row.get("p"));
                });
        testCall(db, "UNWIND [0,1,2,3] as value RETURN apoc.agg.median(value) as p",
                (row) -> {
                    assertEquals(1.5D, row.get("p"));
                });
        testCall(db, "UNWIND [0,1, 2 ,3,4] as value RETURN apoc.agg.median(value) as p",
                (row) -> {
                    assertEquals(2D, row.get("p"));
                });
        testCall(db, "UNWIND [1,1.5, 2,2.5 ,3, 3.5] as value RETURN apoc.agg.median(value) as p",
                (row) -> {
                    assertEquals(2.25D, row.get("p"));
                });
        testCall(db, "UNWIND [1,1.5,2,2.5,3] as value RETURN apoc.agg.median(value) as p",
                (row) -> {
                    assertEquals(2D, row.get("p"));
                });
    }
}
