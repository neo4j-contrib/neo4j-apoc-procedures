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
package apoc.mongodb;

import apoc.util.Util;
import org.bson.Document;

import java.util.Collections;
import java.util.Map;

import static apoc.mongodb.MongoDBUtils.getDocument;

public class MongoDbConfig {

    private final boolean compatibleValues;
    private final boolean extractReferences;
    private final boolean objectIdAsMap;

    private final String idFieldName;

    private final String collection;
    private final Document project;
    private final Document sort;

    private final int skip;
    private final int limit;

    public MongoDbConfig(Map<String, Object> config) {
        if (config == null) config = Collections.emptyMap();
        this.compatibleValues = Util.toBoolean(config.getOrDefault("compatibleValues", true));
        this.extractReferences = Util.toBoolean(config.getOrDefault("extractReferences", false));
        this.objectIdAsMap = Util.toBoolean(config.getOrDefault("objectIdAsMap", true));
        this.idFieldName = (String) config.getOrDefault("idFieldName", "_id");
        this.collection = (String) config.getOrDefault("collection", "");
        this.project = getDocument(config.get("project"));
        this.sort = getDocument(config.get("sort"));
        this.skip = Util.toInteger(config.getOrDefault("skip", 0));
        this.limit = Util.toInteger(config.getOrDefault("limit", 0));
    }

    public boolean isCompatibleValues() {
        return compatibleValues;
    }

    public boolean isExtractReferences() {
        return extractReferences;
    }

    public boolean isObjectIdAsMap() {
        return objectIdAsMap;
    }

    public String getIdFieldName() {
        return idFieldName;
    }

    public String getCollection() {
        return collection;
    }

    public Document getSort() {
        return sort;
    }

    public Document getProject() {
        return project;
    }

    public int getSkip() {
        return skip;
    }

    public int getLimit() {
        return limit;
    }
}
