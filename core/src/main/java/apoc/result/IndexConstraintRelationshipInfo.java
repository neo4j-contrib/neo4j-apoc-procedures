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
package apoc.result;

import java.util.List;

/**
 * Created by alberto.delazzari on 04/07/17.
 */
public class IndexConstraintRelationshipInfo {

    public final String name;

    public final String type;

    public final List<String> properties;

    public final String status;

    public final Object relationshipType;

    public IndexConstraintRelationshipInfo(
            String name, String schemaType, List<String> properties, String status, Object relationshipType) {
        this.name = name;
        this.type = schemaType;
        this.properties = properties;
        this.status = status;
        this.relationshipType = relationshipType;
    }
}
