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

import static apoc.load.util.ConversionUtil.SilentDeserializer;

import apoc.export.util.DurationValueSerializer;
import apoc.export.util.PointSerializer;
import apoc.export.util.TemporalSerializer;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.spi.json.JacksonJsonProvider;
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.temporal.Temporal;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.graphdb.spatial.Point;
import org.neo4j.values.storable.DurationValue;

/**
 * @author mh
 * @since 04.05.16
 */
public class JsonUtil {
    private static final Option[] defaultJsonPathOptions = {Option.DEFAULT_PATH_LEAF_TO_NULL, Option.SUPPRESS_EXCEPTIONS
    };

    public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    public static final String PATH_OPTIONS_ERROR_MESSAGE =
            "Invalid pathOptions. The allowed values are: " + EnumSet.allOf(Option.class);

    static {
        OBJECT_MAPPER.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
        OBJECT_MAPPER.configure(JsonParser.Feature.AUTO_CLOSE_SOURCE, false);
        OBJECT_MAPPER.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
        OBJECT_MAPPER.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
        OBJECT_MAPPER.configure(JsonParser.Feature.ALLOW_COMMENTS, true);
        OBJECT_MAPPER.configure(JsonParser.Feature.ALLOW_BACKSLASH_ESCAPING_ANY_CHARACTER, true);
        OBJECT_MAPPER.configure(JsonParser.Feature.ALLOW_NON_NUMERIC_NUMBERS, true);
        OBJECT_MAPPER.enable(DeserializationFeature.USE_LONG_FOR_INTS);
        SimpleModule module = new SimpleModule("Neo4jApocSerializer");
        module.addSerializer(Point.class, new PointSerializer());
        module.addSerializer(Temporal.class, new TemporalSerializer());
        module.addSerializer(DurationValue.class, new DurationValueSerializer());
        OBJECT_MAPPER.registerModule(module);
    }

    static class NonClosingStream extends FilterInputStream {

        protected NonClosingStream(InputStream in) {
            super(in);
        }

        @Override
        public void close() throws IOException {}
    }

    private static Configuration getJsonPathConfig(List<String> options, ObjectMapper objectMapper) {
        try {
            Option[] opts = options == null
                    ? defaultJsonPathOptions
                    : options.stream().map(Option::valueOf).toArray(Option[]::new);
            return Configuration.builder()
                    .options(opts)
                    .jsonProvider(new JacksonJsonProvider(objectMapper))
                    .mappingProvider(new JacksonMappingProvider(objectMapper))
                    .build();
        } catch (Exception e) {
            throw new RuntimeException(PATH_OPTIONS_ERROR_MESSAGE, e);
        }
    }

    public static Stream<Object> loadJson(String url, Map<String, Object> headers, String payload) {
        return loadJson(url, headers, payload, "", true, null, null);
    }

    public static Stream<Object> loadJson(
            Object urlOrBinary,
            Map<String, Object> headers,
            String payload,
            String path,
            boolean failOnError,
            List<String> options) {
        return loadJson(urlOrBinary, headers, payload, path, failOnError, null, options);
    }

    public static Stream<Object> loadJson(
            Object urlOrBinary,
            Map<String, Object> headers,
            String payload,
            String path,
            boolean failOnError,
            String compressionAlgo,
            List<String> options) {
        try {
            if (urlOrBinary instanceof String) {
                String url = (String) urlOrBinary;
                urlOrBinary = Util.getLoadUrlByConfigFile("json", url, "url").orElse(url);
            }
            InputStream input = FileUtils.inputStreamFor(urlOrBinary, headers, payload, compressionAlgo);
            JsonParser parser = OBJECT_MAPPER.getFactory().createParser(input);
            MappingIterator<Object> it = OBJECT_MAPPER.readValues(parser, Object.class);
            Stream<Object> stream = StreamSupport.stream(Spliterators.spliteratorUnknownSize(it, 0), false);
            return StringUtils.isBlank(path)
                    ? stream
                    : stream.map((value) -> JsonPath.parse(value, getJsonPathConfig(options, OBJECT_MAPPER))
                            .read(path));
        } catch (IOException e) {
            if (!failOnError) {
                return Stream.of();
            } else {
                throw new RuntimeException(e);
            }
        }
    }

    public static Stream<Object> loadJson(String url) {
        return loadJson(url, null, null, "", true, null, null);
    }

    public static <T> T parse(String json, String path, Class<T> type) {
        return parse(json, path, type, null);
    }

    public static <T> T parse(String json, String path, Class<T> type, List<String> options) {
        return parse(json, path, type, options, false);
    }

    public static <T> T parse(
            String json,
            String path,
            Class<T> type,
            List<String> options, /*boolean failOnError,*/
            boolean validation) {

        if (json == null || json.isEmpty()) return null;
        try {
            SilentDeserializer deserializer = validation ? new SilentDeserializer(null, null) : null;
            ObjectMapper objectMapper = getObjectMapper(deserializer);
            final String listOpt = Option.ALWAYS_RETURN_LIST.name();
            if (type == Map.class && options != null && options.contains(listOpt)) {
                throw new RuntimeException(
                        "It's not possible to use " + listOpt + " option because the conversion should return a Map");
            }
            if (path == null || path.isEmpty()) {
                final T t = (T) objectMapper.readValue(json, Object.class);
                return getJson(deserializer, t);
            }
            final T jsonParsed = JsonPath.parse(json, getJsonPathConfig(options, objectMapper))
                    .read(path, type);
            return getJson(deserializer, jsonParsed);
        } catch (IOException e) {
            throw new RuntimeException(
                    "Can't convert " + json + " to " + type.getSimpleName() + " with path " + path, e);
        }
    }

    private static <T> T getJson(SilentDeserializer deserializer, T json) {
        if (deserializer == null) {
            return json;
        }
        return (T) deserializer.getErrorList();
    }

    private static ObjectMapper getObjectMapper(SilentDeserializer deserializer) {
        if (deserializer == null) {
            return OBJECT_MAPPER;
        }
        SimpleModule module = new SimpleModule("SilentModule").addDeserializer(Object.class, deserializer);
        return OBJECT_MAPPER.copy().registerModule(module);
    }

    public static String writeValueAsString(Object json) {
        try {
            return OBJECT_MAPPER.writeValueAsString(json);
        } catch (JsonProcessingException e) {
            return null;
        }
    }

    public static byte[] writeValueAsBytes(Object json) {
        try {
            return OBJECT_MAPPER.writeValueAsBytes(json);
        } catch (JsonProcessingException e) {
            return null;
        }
    }
}
