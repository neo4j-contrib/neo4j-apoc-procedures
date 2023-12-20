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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class QueryUtilTest {

    @Test
    public void shouldReturnTrueForValidQueries() {
        assertTrue(
                QueryUtil.isValidQuery("CREATE USER dummy IF NOT EXISTS SET PASSWORD 'pass12345' CHANGE NOT REQUIRED"));
    }

    @Test
    public void shouldReturnFalseForInvalidQueries() {
        assertFalse(
                QueryUtil.isValidQuery("MATCH USER dummy IF NOT EXISTS SET PASSWORD 'pass12345' CHANGE NOT REQUIRED"));
    }
}
