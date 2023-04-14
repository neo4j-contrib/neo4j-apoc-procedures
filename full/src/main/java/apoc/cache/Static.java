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
package apoc.cache;

import apoc.ApocConfig;
import apoc.Extended;
import apoc.result.KeyValueResult;
import apoc.result.ObjectResult;
import apoc.util.Util;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.*;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

/**
 * @author mh
 * @since 22.05.16
 */
@Extended
public class Static {

    @Context
    public GraphDatabaseAPI db;

    @Context
    public ApocConfig apocConfig;

    private static Map<String,Object> storage = new HashMap<>();

    @Procedure("apoc.static.get")
    @Deprecated
    @Description("apoc.static.get(name) - returns statically stored value from config (apoc.static.<key>) or server lifetime storage")
    public Stream<ObjectResult> getProcedure(@Name("key") String key) {
        return Stream.of(new ObjectResult(storage.getOrDefault(key, fromConfig(key))));
    }

    @UserFunction("apoc.static.get")
    @Description("apoc.static.get(name) - returns statically stored value from config (apoc.static.<key>) or server lifetime storage")
    public Object get(@Name("key") String key) {
        return storage.getOrDefault(key, fromConfig(key));
    }

    @UserFunction("apoc.static.getAll")
    @Description("apoc.static.getAll(prefix) - returns statically stored values from config (apoc.static.<prefix>.*) or server lifetime storage")
    public Map<String,Object> getAll(@Name("prefix") String prefix) {
        return getFromConfigAndStorage(prefix);
    }

    private HashMap<String, Object> getFromConfigAndStorage(@Name("prefix") String prefix) {

        HashMap<String, Object> result = new HashMap<>();
        String configPrefix = prefix.isEmpty() ? "apoc.static": "apoc.static." + prefix;
        Iterators.stream(apocConfig.getKeys(configPrefix)).forEach(s -> result.put(s.substring(configPrefix.length()+1), apocConfig.getString(s)));
        result.putAll(Util.subMap(storage, prefix));
        return result;
    }

    @Procedure("apoc.static.list")
    @Description("apoc.static.list(prefix) - returns statically stored values from config (apoc.static.<prefix>.*) or server lifetime storage")
    public Stream<KeyValueResult> list(@Name("prefix") String prefix) {
        HashMap<String, Object> result = getFromConfigAndStorage(prefix);
        return result.entrySet().stream().map( e -> new KeyValueResult(e.getKey(),e.getValue()));
    }

    private Object fromConfig(@Name("key") String key) {
        return apocConfig.getString("apoc.static."+key);
    }

    @Procedure("apoc.static.set")
    @Description("apoc.static.set(name, value) - stores value under key for server lifetime storage, returns previously stored or configured value")
    public Stream<ObjectResult> set(@Name("key") String key, @Name("value") Object value) {
        Object previous = value == null ? storage.remove(key) : storage.put(key, value);
        return Stream.of(new ObjectResult(previous==null ? fromConfig(key) : previous));
    }

    public static void clear() {
        storage.clear();
    }
}
