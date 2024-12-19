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
package apoc.meta;

import com.google.common.primitives.Booleans;
import com.google.common.primitives.Bytes;
import com.google.common.primitives.Chars;
import com.google.common.primitives.Floats;
import com.google.common.primitives.Shorts;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.spatial.Point;
import org.neo4j.values.storable.DurationValue;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public enum TypesExtended {
    LONG,
    DOUBLE,
    INTEGER,
    FLOAT,
    STRING,
    BOOLEAN,
    RELATIONSHIP,
    NODE,
    PATH,
    NULL,
    ANY,
    MAP,
    LIST,
    POINT,
    DATE,
    DATE_TIME,
    LOCAL_TIME,
    LOCAL_DATE_TIME,
    TIME,
    DURATION;

    private String typeOfList = "ANY";

    private static final Map<Class<?>, Class<?>> primitivesMapping = new HashMap() {
        {
            put(double.class, Double.class);
            put(float.class, Float.class);
            put(int.class, Integer.class);
            put(long.class, Long.class);
            put(short.class, Short.class);
            put(boolean.class, Boolean.class);
        }
    };

    @Override
    public String toString() {
        if (this == TypesExtended.LIST) {
            return "LIST OF " + typeOfList;
        }
        return super.toString();
    }

    public static TypesExtended of(Object value) {
        TypesExtended type = of(value == null ? null : value.getClass());
        if (type == TypesExtended.LIST && !value.getClass().isArray()) {
            type.typeOfList = inferType((List<?>) value);
        }
        return type;
    }

    public static Object[] toObjectArray(Object value) {
        if (value instanceof int[]) {
            return Arrays.stream((int[]) value).boxed().toArray();
        } else if (value instanceof long[]) {
            return Arrays.stream((long[]) value).boxed().toArray();
        } else if (value instanceof double[]) {
            return Arrays.stream((double[]) value).boxed().toArray();
        } else if (value instanceof boolean[]) {
            return Booleans.asList((boolean[]) value).toArray();
        } else if (value instanceof float[]) {
            return Floats.asList((float[]) value).toArray();
        } else if (value instanceof byte[]) {
            return Bytes.asList((byte[]) value).toArray();
        } else if (value instanceof char[]) {
            return Chars.asList((char[]) value).toArray();
        } else if (value instanceof short[]) {
            return Shorts.asList((short[]) value).toArray();
        }
        return value.getClass().isArray() ? (Object[]) value : ((List<Object>) value).toArray();
    }

    public static TypesExtended of(Class<?> type) {
        if (type == null) return NULL;
        if (type.isArray()) {
            TypesExtended innerType = TypesExtended.of(type.getComponentType());
            TypesExtended returnType = LIST;
            returnType.typeOfList = innerType.toString();
            return returnType;
        }
        if (type.isPrimitive()) {
            type = primitivesMapping.getOrDefault(type, type);
        }
        if (Number.class.isAssignableFrom(type)) {
            return Double.class.isAssignableFrom(type) || Float.class.isAssignableFrom(type) ? FLOAT : INTEGER;
        }
        if (Boolean.class.isAssignableFrom(type)) {
            return BOOLEAN;
        }
        if (String.class.isAssignableFrom(type)) {
            return STRING;
        }
        if (Map.class.isAssignableFrom(type)) {
            return MAP;
        }
        if (Node.class.isAssignableFrom(type)) {
            return NODE;
        }
        if (Relationship.class.isAssignableFrom(type)) {
            return RELATIONSHIP;
        }
        if (Path.class.isAssignableFrom(type)) {
            return PATH;
        }
        if (Point.class.isAssignableFrom(type)) {
            return POINT;
        }
        if (List.class.isAssignableFrom(type)) {
            return LIST;
        }
        if (LocalDate.class.isAssignableFrom(type)) {
            return DATE;
        }
        if (LocalTime.class.isAssignableFrom(type)) {
            return LOCAL_TIME;
        }
        if (LocalDateTime.class.isAssignableFrom(type)) {
            return LOCAL_DATE_TIME;
        }
        if (DurationValue.class.isAssignableFrom(type)) {
            return DURATION;
        }
        if (OffsetTime.class.isAssignableFrom(type)) {
            return TIME;
        }
        if (ZonedDateTime.class.isAssignableFrom(type)) {
            return DATE_TIME;
        }
        return ANY;
    }

    public static TypesExtended from(String typeName) {
        if (typeName == null) {
            return STRING;
        }
        typeName = typeName.toUpperCase().replace("_", "");
        for (TypesExtended type : values()) {
            final String name = type.name().replace("_", "");
            // check, e.g. both "LOCAL_DATE_TIME" and "LOCALDATETIME"
            if (name.startsWith(typeName)) {
                return type;
            }
        }
        return STRING;
    }

    public static String inferType(List<?> list) {
        Set<String> set = list.stream().limit(10).map(e -> of(e).name()).collect(Collectors.toSet());
        return set.size() != 1 ? "ANY" : set.iterator().next();
    }
}
