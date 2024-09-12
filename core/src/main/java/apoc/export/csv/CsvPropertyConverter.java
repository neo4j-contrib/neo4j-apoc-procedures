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

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.graphdb.Entity;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.PointValue;

public class CsvPropertyConverter {

    public static boolean addPropertyToGraphEntity(
            Entity entity, CsvHeaderField field, Object value, CsvLoaderConfig config) {
        if (field.isIgnore() || value == null) {
            return false;
        }
        if (field.isArray()) {
            final List list = (List) value;
            final boolean listContainingNull = list.stream().anyMatch(Objects::isNull);
            // todo - to maintain compatibility with neo4j-admin import, we skip ONLY empty cells
            //  while array cell like "...., ,....", will be imported as propKey: [" "]
            //  might be worth add another config to ignore blank item as well, and/or array elements, e.g
            // "...,a;b;;;c,..."
            final boolean isEmptyCell = config.isIgnoreEmptyCellArray() && list.equals(Collections.singletonList(""));
            if (listContainingNull || isEmptyCell) {
                return false;
            }
            final Object[] prototype = getPrototypeFor(field.getType().toUpperCase());
            final Object[] array = list.toArray(prototype);
            entity.setProperty(field.getName(), array);
        } else {
            if (config.isIgnoreBlankString() && value instanceof String && StringUtils.isBlank((String) value)) {
                return false;
            }
            entity.setProperty(field.getName(), value);
        }
        return true;
    }

    public static Object[] getPrototypeFor(String type) {
        switch (type) {
            case "INT":
            case "LONG":
                return new Long[] {};
            case "FLOAT":
            case "DOUBLE":
                return new Double[] {};
            case "BOOLEAN":
                return new Boolean[] {};
            case "BYTE":
                return new Byte[] {};
            case "SHORT":
                return new Short[] {};
            case "CHAR":
                return new Character[] {};
            case "STRING":
                return new String[] {};
            case "DATETIME":
                return new ZonedDateTime[] {};
            case "LOCALTIME":
                return new LocalTime[] {};
            case "LOCALDATETIME":
                return new LocalDateTime[] {};
            case "POINT":
                return new PointValue[] {};
            case "TIME":
                return new OffsetTime[] {};
            case "DATE":
                return new LocalDate[] {};
            case "DURATION":
                return new DurationValue[] {};
        }
        throw new IllegalStateException("Type " + type + " not supported.");
    }
}
