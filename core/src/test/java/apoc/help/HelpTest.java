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
package apoc.help;

import apoc.bitwise.BitwiseOperations;
import apoc.coll.Coll;
import apoc.util.TestUtil;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import static apoc.util.Util.map;
import static org.junit.Assert.assertEquals;

/**
 * @author mh
 * @since 06.11.16
 */
public class HelpTest {

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();

    @Before
    public void setUp() throws Exception {
        TestUtil.registerProcedure(db, Help.class, BitwiseOperations.class,Coll.class);
    }

    @Test
    public void info() throws Exception {
        TestUtil.testCall(db,"CALL apoc.help($text)",map("text","bitwise"), (row) -> {
            assertEquals("function",row.get("type"));
            assertEquals("apoc.bitwise.op",row.get("name"));
            assertEquals(true, ((String) row.get("text")).contains("bitwise operations"));
        });
        TestUtil.testCall(db,"CALL apoc.help($text)",map("text","operations+"), (row) -> assertEquals("apoc.bitwise.op",row.get("name")));
        TestUtil.testCall(db,"CALL apoc.help($text)",map("text","toSet"), (row) -> {
            assertEquals("function",row.get("type"));
            assertEquals("apoc.coll.toSet",row.get("name"));
            assertEquals(true, ((String) row.get("text")).contains("unique list"));
        });
    }

    @Test
    public void indicateCore() {
        TestUtil.testCall(db,"CALL apoc.help($text)",map("text","coll.zipToRows"), (row) -> {
            assertEquals(true, row.get("core"));
        });
    }

}
