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

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class CsvHeaderFields {

    /**
     * Processes CSV header. Works for both nodes and relationships.
     *
     * @param header
     * @param delimiter
     * @param quotationCharacter
     * @return
     */
    public static List<CsvHeaderField> processHeader(final String header, final char delimiter, final char quotationCharacter) {
        final String separatorRegex = Pattern.quote(String.valueOf(delimiter));
        final List<String> attributes = Arrays.asList(header.split(separatorRegex));

        final List<CsvHeaderField> fieldEntries =
                IntStream.range(0, attributes.size())
                        .mapToObj(i -> CsvHeaderField.parse(i, attributes.get(i), quotationCharacter))
                        .collect(Collectors.toList());

        return fieldEntries;
    }

}
