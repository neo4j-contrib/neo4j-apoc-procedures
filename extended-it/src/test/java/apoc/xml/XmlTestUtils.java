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
package apoc.xml;

import java.util.Arrays;
import java.util.Map;

import static apoc.util.MapUtil.map;
import static java.util.Arrays.asList;

public class XmlTestUtils {

    public static final Map<String, Object> XML_XPATH_AS_NESTED_MAP = map(
            "_type",
            "book",
            "id",
            "bk103",
            "_children",
            Arrays.asList(
                    map("_type", "author", "_text", "Corets, Eva"),
                    map("_type", "title", "_text", "Maeve Ascendant"),
                    map("_type", "genre", "_text", "Fantasy"),
                    map("_type", "price", "_text", "5.95"),
                    map("_type", "publish_date", "_text", "2000-11-17"),
                    map(
                            "_type",
                            "description",
                            "_text",
                            "After the collapse of a nanotechnology society in England, the young survivors lay the foundation for a new society.")));
}
