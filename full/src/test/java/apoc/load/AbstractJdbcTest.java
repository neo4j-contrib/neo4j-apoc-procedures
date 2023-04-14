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
package apoc.load;

import apoc.util.Util;

import java.time.Instant;
import java.util.Calendar;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public abstract class AbstractJdbcTest {

    protected static java.sql.Date hireDate = new java.sql.Date(new Calendar.Builder().setDate(2017, 04, 25).build().getTimeInMillis());

    protected static java.sql.Timestamp effectiveFromDate = java.sql.Timestamp.from(Instant.parse("2016-06-22T17:10:25Z"));

    protected static java.sql.Time time = java.sql.Time.valueOf("15:37:00");

    public void assertResult(Map<String, Object> row) {
        Map<String, Object> expected = Util.map("NAME", "John", "SURNAME", null, "HIRE_DATE", hireDate.toLocalDate(), "EFFECTIVE_FROM_DATE",
                effectiveFromDate.toLocalDateTime(), "TEST_TIME", time.toLocalTime(), "NULL_DATE", null);
        assertEquals(expected, row.get("row"));
    }
}
