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

import java.util.regex.Pattern;

public class CsvLoaderConstants {

    // we can have a pattern like "prop2:time{timezone:+02:00}", so in this case {} is recognized by <optPar>
    // and handled through KEY_VALUE_PATTERN (with pattern: {key1:value1, key1:value2}), that it's used similarly to
    // https://neo4j.com/docs/operations-manual/current/tools/neo4j-admin-import/#import-tool-header-format-properties
    public static final Pattern FIELD_PATTERN = Pattern.compile(
            "^(?<name>[^:]*)(:(?<type>\\w+))?(?<optPar>\\{.*})?(\\((?<idspace>[-a-zA-Z_0-9]+)\\))?(?<array>\\[\\])?$");
    public static final Pattern KEY_VALUE_PATTERN =
            Pattern.compile("(?:\\A|,)\\s*+(?<key>[a-z_A-Z]\\w*+)\\s*:\\s*(?<value>[^\\s,]+)");

    public static final String ARRAY_PATTERN = "[]";

    public static final String IGNORE_FIELD = "IGNORE";
    public static final String ID_FIELD = "ID";
    public static final String START_ID_FIELD = "START_ID";
    public static final String END_ID_FIELD = "END_ID";
    public static final String LABEL_FIELD = "LABEL";
    public static final String TYPE_FIELD = "TYPE";

    public static final String IDSPACE_ATTR_PREFIX = "__csv";
    public static final String DEFAULT_IDSPACE = "__CSV_DEFAULT_IDSPACE";

    public static final String ID_ATTR = IDSPACE_ATTR_PREFIX + "_" + ID_FIELD.toLowerCase();
    public static final String START_ID_ATTR = IDSPACE_ATTR_PREFIX + "_" + START_ID_FIELD.toLowerCase();
    public static final String END_ID_ATTR = IDSPACE_ATTR_PREFIX + "_" + END_ID_FIELD.toLowerCase();
    public static final String LABEL_ATTR = IDSPACE_ATTR_PREFIX + "_" + LABEL_FIELD.toLowerCase();
    public static final String TYPE_ATTR = IDSPACE_ATTR_PREFIX + "_" + TYPE_FIELD.toLowerCase();
}
