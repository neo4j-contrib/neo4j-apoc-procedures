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
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.Arrays;
import java.util.Collection;

import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testCall;
import static org.junit.Assert.assertEquals;

/**
 * @author Stefan Armbruster
 */
@RunWith(Parameterized.class)
public class StringCleanTest {

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    @BeforeClass
    public static void setUp() throws Exception {
        TestUtil.registerProcedure(db, Strings.class);
    }

    @Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                { "&N[]eo  4 #J-(3.0)  ", "neo4j30"},
                { "German umlaut Ä Ö Ü ä ö ü ß ", "germanumlautaeoeueaeoeuess" },
                { "French çÇéèêëïîôœàâæùûü", "frenchcceeeeiioœaaæuuue"}
        });
    }

    @Parameter(value = 0)
    public String dirty;

    @Parameter(value = 1)
    public String clean;

    @Test
    public void testClean() throws Exception {
        testCall(db,
                "RETURN apoc.text.clean($a) AS value",
                map("a", dirty),
                row -> assertEquals(clean, row.get("value")));
    }

}
