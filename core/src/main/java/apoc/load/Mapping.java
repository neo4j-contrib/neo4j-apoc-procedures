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

import apoc.load.util.LoadCsvConfig;
import apoc.meta.Meta;
import apoc.util.Util;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.values.storable.DateTimeValue;
import org.neo4j.values.storable.DateValue;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.LocalDateTimeValue;
import org.neo4j.values.storable.LocalTimeValue;
import org.neo4j.values.storable.TimeValue;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static apoc.ApocConfig.apocConfig;
import static apoc.util.Util.parseCharFromConfig;
import static java.util.Collections.emptyList;
import static org.neo4j.configuration.GraphDatabaseSettings.db_temporal_timezone;

public class Mapping {
    public static final Mapping EMPTY = new Mapping("", Collections.emptyMap(), LoadCsvConfig.DEFAULT_ARRAY_SEP, false);
    final String name;
    final Collection<String> nullValues;
    final Meta.Types type;
    final boolean array;
    final boolean ignore;
    final char arraySep;
    private final Pattern arrayPattern;
    private final Map<String, Object> optionalData;

    public Mapping(String name, Map<String, Object> mapping, char arraySep, boolean ignore) {
        this.name = mapping.getOrDefault("name", name).toString();
        this.array = (Boolean) mapping.getOrDefault("array", false);
        this.optionalData = (Map<String, Object>) mapping.get("optionalData");
        
        this.ignore = (Boolean) mapping.getOrDefault("ignore", ignore);
        this.nullValues = (Collection<String>) mapping.getOrDefault("nullValues", emptyList());
        this.arraySep = parseCharFromConfig(mapping, "arraySep", arraySep);
        this.type = Meta.Types.from(mapping.getOrDefault("type", "STRING").toString());
        this.arrayPattern = Pattern.compile(String.valueOf(this.arraySep), Pattern.LITERAL);

        if (this.type == null) {
            // Call this out to the user explicitly because deep inside of LoadCSV and others you will get
            // NPEs that are hard to spot if this is allowed to go through.
            throw new RuntimeException("In specified mapping, there is no type by the name " +
                    mapping.getOrDefault("type", "STRING").toString());
        }
    }

    public Object convert(String value) {
        return array ? convertArray(value) : convertType(value);
    }

    private Object convertArray(String value) {
        String[] values = arrayPattern.split(value);
        List<Object> result = new ArrayList<>(values.length);
        for (String v : values) {
            result.add(convertType(v));
        }
        return result;
    }

    private Object convertType(String value) {
        if (nullValues.contains(value) || (StringUtils.isBlank(value) && !type.equals(Meta.Types.STRING))) {
            return null;
        }
        if (type == Meta.Types.STRING) return value;

        final Supplier<ZoneId> timezone = () -> ZoneId.of((String) optionalData.getOrDefault("timezone", apocConfig().getString(db_temporal_timezone.name())));
        switch (type) {
            case POINT:
                return Util.toPoint(Util.fromJson(value, Map.class), optionalData);
            case LOCAL_DATE_TIME:
                // asObjectCopy() returns LocalDateTime, 
                // because in case of array entity.setProperty() fails with LocalDateTimeValue[]
                return LocalDateTimeValue.parse(value).asObjectCopy();
            case LOCAL_TIME:
                return LocalTimeValue.parse(value).asObjectCopy();
            case DATE_TIME:
                return DateTimeValue.parse(value, timezone).asObjectCopy();
            case TIME:
                return TimeValue.parse(value, timezone).asObjectCopy();
            case DATE:
                return DateValue.parse(value).asObjectCopy();
            case DURATION:
                return DurationValue.parse(value);
            case INTEGER: return Util.toLong(value);
            case FLOAT: return Util.toDouble(value);
            case BOOLEAN: return Util.toBoolean(value);
            case NULL: return null;
            case LIST: return Arrays.stream(arrayPattern.split(value)).map(this::convertType).collect(Collectors.toList());
            default: return value;
        }
    }
}
