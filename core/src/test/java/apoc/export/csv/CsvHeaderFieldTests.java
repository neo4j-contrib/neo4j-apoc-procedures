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
package apoc.export.csv;

import apoc.meta.Meta;
import org.junit.Test;

import java.util.regex.Matcher;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class CsvHeaderFieldTests {

    public static final String TEST_NAME = "name";
    public static final String TEST_TYPE = "Type";
    public static final String TEST_IDSPACE = "IDSPACE";
    public static final String TEST_ARRAY = "[]";
    private static final String TEST_OPT_PAR = "{crs:WGS-84}";

    public static final String TEST_FIELD_1 = "name:Type(IDSPACE)[]";
    public static final String TEST_FIELD_2 = "name:Type(IDSPACE)";
    public static final String TEST_FIELD_3 = "name:Type";
    public static final String TEST_FIELD_4 = "name";
    public static final String TEST_FIELD_5 = "name:Type{crs:WGS-84}(IDSPACE)[]";

    @Test
    public void testCsvField1() {
        CsvHeaderField field = CsvHeaderField.parse(0, TEST_FIELD_1, '"');
        assertEquals(TEST_NAME,    field.getName());
        assertEquals(TEST_TYPE,    field.getType());
        assertEquals(TEST_IDSPACE, field.getIdSpace());
        assertTrue(field.isArray());
    }

    @Test
    public void testCsvField2() {
        CsvHeaderField field = CsvHeaderField.parse(0, TEST_FIELD_2, '"');
        assertEquals(TEST_NAME,    field.getName());
        assertEquals(TEST_TYPE,    field.getType());
        assertEquals(TEST_IDSPACE, field.getIdSpace());
        assertFalse(field.isArray());
    }

    @Test
    public void testCsvField3() {
        CsvHeaderField field = CsvHeaderField.parse(0, TEST_FIELD_3, '"');
        assertEquals(TEST_NAME, field.getName());
        assertEquals(TEST_TYPE, field.getType());
        assertEquals(CsvLoaderConstants.DEFAULT_IDSPACE, field.getIdSpace());
        assertFalse(field.isArray());
    }

    @Test
    public void testCsvField4() {
        CsvHeaderField field = CsvHeaderField.parse(0, TEST_FIELD_4, '"');
        assertEquals(TEST_NAME, field.getName());
        assertEquals(Meta.Types.STRING.name(), field.getType());
        assertEquals(CsvLoaderConstants.DEFAULT_IDSPACE, field.getIdSpace());
        assertFalse(field.isArray());
    }

    @Test
    public void testNamedGroups() {
        Matcher matcher = CsvLoaderConstants.FIELD_PATTERN.matcher(TEST_FIELD_1);

        assertTrue(matcher.find());
        assertEquals(7, matcher.groupCount());
        assertNull(matcher.group("optPar"));
        assertEquals(TEST_NAME, matcher.group("name"));
        assertEquals(TEST_TYPE, matcher.group("type"));
        assertEquals(TEST_IDSPACE, matcher.group("idspace"));
        assertEquals(TEST_ARRAY, matcher.group("array"));
    }

    @Test
    public void testOptionalParamGroup() {
        Matcher matcher = CsvLoaderConstants.FIELD_PATTERN.matcher(TEST_FIELD_5);

        assertTrue(matcher.find());
        assertEquals(7, matcher.groupCount());
        assertEquals(TEST_OPT_PAR, matcher.group("optPar"));
        assertEquals(TEST_NAME, matcher.group("name"));
        assertEquals(TEST_TYPE, matcher.group("type"));
        assertEquals(TEST_IDSPACE, matcher.group("idspace"));
        assertEquals(TEST_ARRAY, matcher.group("array"));
    }

}
