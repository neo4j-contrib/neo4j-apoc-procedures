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
package apoc.meta.tablesforlabels;

import java.util.*;

public class PropertyTracker {
    public String name;
    public Set<String> types;
    public boolean mandatory;
    public long observations;
    public long nulls;

    public PropertyTracker(String name) {
        this.name = name;
        types = new HashSet<>(3);
        mandatory = false;
        observations = 0L;
        nulls = 0L;
    }

    public void addObservation(Object value) {
        observations++;
        if (value == null) { nulls++; }
        types.add(assignTypeName(value));
    }

    private String assignTypeName(Object value) {
        String typeName = value.getClass().getCanonicalName();
        if (typeMappings.containsKey(typeName)) {
            return typeMappings.get(typeName);
        }

        return typeName.replace("java.lang.", "");
    }

    public List<String> propertyTypes() {
        List<String> ret = new ArrayList<>(types);
        Collections.sort(ret);
        return ret;
    }

    public static final Map<String,String> typeMappings = new HashMap<String,String>();
    static {
        typeMappings.put("java.lang.String", "String");
        typeMappings.put("java.lang.String[]", "StringArray");
        typeMappings.put("java.lang.Double", "Double");
        typeMappings.put("java.lang.Double[]", "DoubleArray");
        typeMappings.put("double[]", "DoubleArray");
        typeMappings.put("java.lang.Integer", "Integer");
        typeMappings.put("java.lang.Integer[]", "IntegerArray");
        typeMappings.put("int[]", "IntegerArray");
        typeMappings.put("java.lang.Long", "Long");
        typeMappings.put("java.lang.Long[]", "LongArray");
        typeMappings.put("long[]", "LongArray");
        typeMappings.put("org.neo4j.values.storable.PointValue", "Point");
        typeMappings.put("org.neo4j.values.storable.PointValue[]", "PointArray");
        typeMappings.put("java.time.ZonedDateTime", "DateTime");
        typeMappings.put("java.time.ZonedDateTime[]", "DateTimeArray");
        typeMappings.put("java.lang.Boolean", "Boolean");
        typeMappings.put("java.lang.Boolean[]", "BooleanArray");
        typeMappings.put("boolean", "Boolean");
        typeMappings.put("boolean[]", "BooleanArray");
        typeMappings.put("java.time.LocalDate", "Date");
        typeMappings.put("java.time.LocalDate[]", "DateArray");
        typeMappings.put("java.time.LocalDateTime", "LocalDateTime");
        typeMappings.put("java.time.LocalDateTime[]", "LocalDateTimeArray");
        typeMappings.put("java.time.LocalTime", "LocalTime");
        typeMappings.put("java.time.LocalTime[]", "LocalTimeArray");
        typeMappings.put("org.neo4j.values.storable.DurationValue", "Duration");
        typeMappings.put("org.neo4j.values.storable.DurationValue[]", "DurationArray");
        typeMappings.put("java.time.OffsetTime", "Time");
        typeMappings.put("java.time.OffsetTime[]", "TimeArray");
    }
}
