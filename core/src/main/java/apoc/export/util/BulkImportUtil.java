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
package apoc.export.util;

import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.PointValue;

import java.time.*;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class BulkImportUtil {

    private static Map<Class<?>, String> allowedMapping = Collections.unmodifiableMap(new HashMap(){{
        put(Double.class, "double");
        put(Float.class, "float");
        put(Integer.class, "int");
        put(Long.class, "long");
        put(Short.class, "short");
        put(Character.class, "char");
        put(Byte.class, "byte");
        put(Boolean.class, "boolean");
        put(DurationValue.class, "duration");
        put(PointValue.class, "point");
        put(LocalDate.class, "date");
        put(LocalDateTime.class, "localdatetime");
        put(LocalTime.class, "localtime");
        put(ZonedDateTime.class, "datetime");
        put(OffsetTime.class, "time");
    }});


    public static String formatHeader(Map.Entry<String, Class> r) {
        if (allowedMapping.containsKey(r.getValue())) {
            return r.getKey() + ":" + allowedMapping.get(r.getValue());
        } else {
            return r.getKey();
        }
    }

}
