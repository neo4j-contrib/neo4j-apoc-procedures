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
package apoc.util;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.TemporalAccessor;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static apoc.util.Util.getFormat;

public class DateParseUtil {

    private static Map<Class<? extends TemporalAccessor>, MethodHandle> parseDateMap = new ConcurrentHashMap<>();
    private static Map<Class<? extends TemporalAccessor>, MethodHandle> simpleParseDateMap = new ConcurrentHashMap<>();
    private static String METHOD_NAME = "parse";

    public static TemporalAccessor dateParse(String value, Class<? extends TemporalAccessor> date, String...formats) {
        try {
            if (formats != null && formats.length > 0) {
                for (String form : formats) {
                    try {
                        try {
                            return getParse(date, getFormat(form), value);
                        } catch (DateTimeParseException e) {
                            return getParse(date, value);
                        }
                    } catch (Exception e) {
                        // Ignore here as we are in a loop checking formats
                        // if all formats fail, a later exception will be called
                        continue;
                    }
                }
            } else {
                return getParse(date, value);
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        } catch (Throwable throwable) {
            throw new RuntimeException(throwable.getMessage());
        }
        throw new RuntimeException("Can't format the date with the pattern");
    }

    private static TemporalAccessor getParse(Class<? extends TemporalAccessor> date, DateTimeFormatter format, String value) throws Throwable {

        MethodHandle methodHandle = parseDateMap.computeIfAbsent(date, method -> {
            MethodHandles.Lookup lookup = MethodHandles.publicLookup();
            try {
                return lookup.findStatic(date, METHOD_NAME, MethodType.methodType(date, CharSequence.class, DateTimeFormatter.class));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        return (TemporalAccessor) methodHandle.invokeWithArguments(value, format);
    }

    private static TemporalAccessor getParse(Class<? extends TemporalAccessor> date, String value) throws Throwable {
        MethodHandle methodHandleSimple = simpleParseDateMap.computeIfAbsent(date, method -> {
            MethodHandles.Lookup lookup = MethodHandles.publicLookup();
            try {
                return lookup.findStatic(date, METHOD_NAME, MethodType.methodType(date, CharSequence.class));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        return (TemporalAccessor) methodHandleSimple.invokeWithArguments(value);
    }

}
