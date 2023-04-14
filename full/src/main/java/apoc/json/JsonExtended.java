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
package apoc.json;

import apoc.Extended;
import apoc.result.StringResult;
import apoc.util.JsonUtil;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.List;
import java.util.stream.Stream;


@Extended
public class JsonExtended {

    @Context
    public org.neo4j.graphdb.GraphDatabaseService db;

    @Procedure
    @Description("apoc.json.validate('{json}' [,'json-path' , 'path-options']) - to check if the json is correct (returning an empty result) or not")
    public Stream<StringResult> validate(@Name("json") String json, @Name(value = "path",defaultValue = "$") String path, @Name(value = "pathOptions", defaultValue = "null") List<String> pathOptions) {
        return ((List<String>) JsonUtil.parse(json, path, Object.class, pathOptions, true))
                .stream()
                .map(StringResult::new);
    }
    
}
