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
package apoc.util;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Collection;
import org.junit.AfterClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

@RunWith(Parameterized.class)
public class UtilQuoteTest {

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    @AfterClass
    public static void teardown() {
        db.shutdown();
    }

    @Parameterized.Parameters
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][] {
            {"abc", true},
            {"_id", true},
            {"some_var", true},
            {"$lock", false},
            {"has$inside", false},
            {"ähhh", true},
            {"rübe", true},
            {"rådhuset", true},
            {"1first", false},
            {"first1", true},
            {"a weird identifier", false},
            {"^n", false},
            {"$$n", false},
            {" var ", false},
            {"foo.bar.baz", false},
        });
    }

    private final String identifier;
    private final boolean shouldAvoidQuote;

    public UtilQuoteTest(String identifier, boolean shouldAvoidQuote) {
        this.identifier = identifier;
        this.shouldAvoidQuote = shouldAvoidQuote;
    }

    @Test
    public void shouldQuoteIfNeededForUsageAsParameterName() {
        db.executeTransactionally(String.format("CREATE (n:TestNode) SET n.%s = true", Util.quote(identifier)));
        // If the query did not fail entirely, did it create the expected property?
        TestUtil.testCallCount(db, String.format("MATCH (n:TestNode) WHERE n.`%s` RETURN id(n)", identifier), 1);
    }

    @Test
    public void shouldNotQuoteWhenAvoidQuoteIsTrue() {
        final String expectedIdentifier = shouldAvoidQuote ? identifier : '`' + identifier + '`';

        assertEquals(expectedIdentifier, Util.quote(identifier));
    }
}
