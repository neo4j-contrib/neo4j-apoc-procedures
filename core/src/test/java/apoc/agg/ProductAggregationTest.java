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

public class ProductAggregationTest {

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    @BeforeClass public static void setUp() throws Exception {
        TestUtil.registerProcedure(db, Product.class);
    }

    @Test
    public void testProduct() throws Exception {
        testCall(db, "UNWIND [] as value RETURN apoc.agg.product(value) as p",
                (row) -> {
                    assertEquals(0D, row.get("p"));
                });
        testCall(db, "UNWIND RANGE(0,3) as value RETURN apoc.agg.product(value) as p",
                (row) -> {
                    assertEquals(0L, row.get("p"));
                });
        testCall(db, "UNWIND RANGE(1,3) as value RETURN apoc.agg.product(value) as p",
                (row) -> {
                    assertEquals(6L, row.get("p"));
                });
        testCall(db, "UNWIND RANGE(2,6) as value RETURN apoc.agg.product(value/2.0) as p",
                (row) -> {
                    assertEquals(22.5D, row.get("p"));
                });
    }
}
