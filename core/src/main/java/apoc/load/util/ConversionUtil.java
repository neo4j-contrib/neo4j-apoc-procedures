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
package apoc.load.util;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.deser.std.UntypedObjectDeserializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ConversionUtil {
    public static final String ERR_PREFIX = "Error with key ";

    public final static class SilentDeserializer extends UntypedObjectDeserializer {
        private final List<String> errorList = new ArrayList<>();

        public SilentDeserializer(JavaType listType, JavaType mapType) {
            super(listType, mapType);
        }

        @Override
        public Object deserialize(JsonParser p, DeserializationContext ctxt) {
            try {
                // fallback to standard deserialization
                return super.deserialize(p, ctxt);
            } catch (IOException e) {
                final String errMsg = ERR_PREFIX + p.getParsingContext().getCurrentName() + "; The error is: " + e.getMessage();
                errorList.add(errMsg);
                return null;
            }
        }

        public List<String> getErrorList() {
            return errorList;
        }
    }

}
