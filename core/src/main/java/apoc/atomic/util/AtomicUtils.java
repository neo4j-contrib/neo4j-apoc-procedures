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
package apoc.atomic.util;

/**
 * @author AgileLARUS
 *
 * @since 3.0.0
 */
public class AtomicUtils {

    public static Number sum(Number oldValue, Number number) {
        if (oldValue instanceof Long) return oldValue.longValue() + number.longValue();
        if (oldValue instanceof Integer) return oldValue.intValue() + number.intValue();
        if (oldValue instanceof Double) return oldValue.doubleValue() + number.doubleValue();
        if (oldValue instanceof Float) return oldValue.floatValue() + number.floatValue();
        if (oldValue instanceof Short) return oldValue.shortValue() + number.shortValue();
        if (oldValue instanceof Byte) return oldValue.byteValue() + number.byteValue();
        return null;
    }

    public static Number sub(Number oldValue, Number number) {
        if (oldValue instanceof Long) return oldValue.longValue() - number.longValue();
        if (oldValue instanceof Integer) return oldValue.intValue() - number.intValue();
        if (oldValue instanceof Double) return oldValue.doubleValue() - number.doubleValue();
        if (oldValue instanceof Float) return oldValue.floatValue() - number.floatValue();
        if (oldValue instanceof Short) return oldValue.shortValue() - number.shortValue();
        if (oldValue instanceof Byte) return oldValue.byteValue() - number.byteValue();
        return null;
    }
}
