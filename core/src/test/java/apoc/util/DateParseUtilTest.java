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

import org.junit.Test;

import java.time.*;

import static apoc.util.DateParseUtil.dateParse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DateParseUtilTest {

    private String[] parseList = new String[]{"wrongPath", "dd-MM-yyyy", "dd/MM/yyyy", "yyyy/MM/dd'T'HH:mm:ss", "yyyy/dd/MM", "iso_zoned_date_time", "yyyy-MM-dd HH:mm", "HH mm"};

    @Test
    public void dateParseTest() {
        assertEquals(LocalDate.of(2018,01,01),dateParse("2018/01/01", LocalDate.class, parseList));
        assertEquals(ZonedDateTime.of(2011,01,01,12,0,0,53810000,ZoneOffset.of("+01:00")),dateParse("2011-01-01T12:00:00.05381+01:00", ZonedDateTime.class, parseList));
        assertEquals(LocalDateTime.of(2018,05,10,12,10),dateParse("2018-05-10 12:10", LocalDateTime.class, parseList));
        assertEquals(LocalTime.of(12,10),dateParse("12 10", LocalTime.class, parseList));
        assertEquals(OffsetTime.of(10,15,30,0,ZoneOffset.of("+01:00")),dateParse("10:15:30+01:00", OffsetTime.class, parseList));
    }

    @Test(expected = RuntimeException.class)
    public void dateParseErrorTest() throws Exception {
        try {
            dateParse("10/01/2010", LocalDateTime.class, parseList);
        } catch (Exception e) {
            assertTrue(e instanceof RuntimeException);
            assertEquals("Can't format the date with the pattern", e.getMessage());
            throw e;
        }
    }
}