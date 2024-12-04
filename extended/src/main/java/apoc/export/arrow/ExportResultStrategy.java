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
package apoc.export.arrow;

import apoc.meta.TypesExtended;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.neo4j.graphdb.GraphDatabaseService;

import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static apoc.export.arrow.ExportArrowStrategy.fromMetaType;
import static apoc.export.arrow.ExportArrowStrategy.toField;

public interface ExportResultStrategy {

    default Schema schemaFor(GraphDatabaseService db, List<Map<String, Object>> records) {
        final List<Field> fields = records.stream()
                .flatMap(m -> m.entrySet().stream())
                .map(e -> new AbstractMap.SimpleEntry<>(e.getKey(), fromMetaType(TypesExtended.of(e.getValue()))))
                .collect(Collectors.groupingBy(
                        e -> e.getKey(), Collectors.mapping(e -> e.getValue(), Collectors.toSet())))
                .entrySet()
                .stream()
                .map(e -> toField(e.getKey(), e.getValue()))
                .collect(Collectors.toList());
        return new Schema(fields);
    }
}
