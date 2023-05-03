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
package apoc.help;

import java.util.List;
import java.util.Map;

/**
 * @author mh
 * @since 11.04.16
 */
public class HelpResult {
    public String type;
    public String name;
    public String text;
    public String signature;
    public List<String> roles;
    public Boolean writes;
    public Boolean core;

    public HelpResult(String type, String name, String text, String signature, List<String> roles, Boolean writes, Boolean core) {
        this.type = type;
        this.name = name;
        this.text = text;
        this.signature = signature;
        this.roles = roles;
        this.writes = writes;
        this.core = core;
    }

    public HelpResult(Map<String, Object> row, Boolean core) {
        this((String)row.get("type"),(String)row.get("name"),(String)row.get("description"),(String)row.get("signature"),null,(Boolean)row.get("writes"), core);
    }
}
