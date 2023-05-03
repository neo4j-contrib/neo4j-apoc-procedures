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
package apoc.label;

import apoc.util.TestUtil;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import static apoc.util.TestUtil.testCall;
import static org.junit.Assert.assertEquals;

public class LabelTest {

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    @BeforeClass
    public static void setUp() throws Exception {
        TestUtil.registerProcedure(db, Label.class);
    }

    @Test
    public void testVerifyNodeLabelExistence() throws Exception {

        db.executeTransactionally("create (a:Person{name:'Foo'})");

        testCall(db, "MATCH (a) RETURN apoc.label.exists(a, 'Person') as value",
                (row) -> {
                    assertEquals(true, row.get("value"));
                });
        testCall(db, "MATCH (a) RETURN apoc.label.exists(a, 'Dog') as value",
                (row) -> {
                    assertEquals(false, row.get("value"));
                });
    }

    @Test
    public void testVerifyRelTypeExistence() throws Exception {

        db.executeTransactionally("create (a:Person{name:'Foo'}), (b:Person{name:'Bar'}), (a)-[:LOVE{since:2010}]->(b)");

        testCall(db, "MATCH ()-[a]->() RETURN apoc.label.exists(a, 'LOVE') as value",
                (row) -> {
                    assertEquals(true, row.get("value"));
                });
        testCall(db, "MATCH ()-[a]->() RETURN apoc.label.exists(a, 'LIVES_IN') as value",
                (row) -> {
                    assertEquals(false, row.get("value"));
                });

    }
}
