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
package apoc.extended.meta;

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
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.spatial.Point;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.VectorValue;

public enum Types {
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
    DURATION,
    VECTOR;

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
        if (this == Types.LIST) {
            return "LIST OF " + typeOfList;
        }
        return super.toString();
    }

    public static Types of(Object value) {
        Types type = of(value == null ? null : value.getClass());
        if (type == Types.LIST && !value.getClass().isArray()) {
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
        } else if (value instanceof boolean[] bools) {
            final var result = new Boolean[bools.length];
            for (int i = 0; i < bools.length; i++) result[i] = bools[i];
            return result;
        } else if (value instanceof float[] floats) {
            final var result = new Float[floats.length];
            for (int i = 0; i < floats.length; i++) result[i] = floats[i];
            return result;
        } else if (value instanceof byte[] bytes) {
            final var result = new Byte[bytes.length];
            for (int i = 0; i < bytes.length; i++) result[i] = bytes[i];
            return result;
        } else if (value instanceof char[] chars) {
            final var result = new Character[chars.length];
            for (int i = 0; i < chars.length; i++) result[i] = chars[i];
            return result;
        } else if (value instanceof short[] shorts) {
            final var result = new Short[shorts.length];
            for (int i = 0; i < shorts.length; i++) result[i] = shorts[i];
            return result;
        }
        return value.getClass().isArray() ? (Object[]) value : ((List<Object>) value).toArray();
    }

    public static Types of(Class<?> type) {
        if (type == null) return NULL;
        if (type.isArray()) {
            Types innerType = Types.of(type.getComponentType());
            Types returnType = LIST;
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
        // The functions using this are deprecated, so give minimal support
        // They should be using Cypher anyway
        if (VectorValue.class.isAssignableFrom(type)) {
            return VECTOR;
        }
        return ANY;
    }

    public static Types from(String typeName) {
        if (typeName == null) {
            return STRING;
        }
        typeName = typeName.toUpperCase().replace("_", "");
        for (Types type : values()) {
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
