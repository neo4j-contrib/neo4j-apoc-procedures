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

import apoc.util.JsonUtil;
import apoc.util.MissingDependencyException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.bson.Document;

public class MongoDBUtils {
    public static final String MONGO_MISSING_DEPS_ERROR = "Cannot find the jar into the plugins folder. \n"
            + "Please put the apoc-mongodb-dependencies-5.x.x-all.jar into plugin folder.\n"
            + "See the documentation: See the documentation: https://neo4j.com/labs/apoc/5/database-integration/mongo/#mongodb-dependencies";

    protected static Document getDocument(Object query) {
        if (query == null) {
            return new Document();
        }
        String json = query instanceof String
                ? (String) query
                : JsonUtil.writeValueAsString(query);

        json = adaptLegacyDocuments(json);

        return Document.parse(json);
    }

    /**
     * In case someone use an old notation, e.g. {`$binary`: $bytes, `$subType`: '00'}
     */
    private static String adaptLegacyDocuments(String json) {
        return json.replace("'$subType'", "'$type'")
                .replace("\"$subType\"", "\"$type\"");
    }

    protected static List<Document> getDocuments(List<Map<String, Object>> pipeline) {
        return pipeline.stream().map(MongoDBUtils::getDocument).collect(Collectors.toList());
    }

    protected static MongoDbConfig getMongoConfig(Map<String, Object> config) {
        try {
            return new MongoDbConfig(config);
        } catch (NoClassDefFoundError e) {
            throw new MissingDependencyException(MONGO_MISSING_DEPS_ERROR);
        }
    }
}
