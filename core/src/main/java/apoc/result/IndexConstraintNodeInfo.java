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
public class IndexConstraintNodeInfo {

    public final String name;

    public final Object label;

    public final List<String> properties;

    public final String status;

    public final String type;

    public final String failure;

    public final double populationProgress;

    public final long size;

    public final double valuesSelectivity;

    public final String userDescription;

    /**
     * Default constructor
     *
     * @param name
     * @param label
     * @param properties
     * @param status status of the index, if it's a constraint it will be empty
     * @param schemaType if it is an index type will be "INDEX" otherwise it will be the type of constraint
     * @param failure
     * @param populationProgress
     * @param size
     * @param userDescription
     */
    public IndexConstraintNodeInfo(String name, Object label, List<String> properties, String status, String schemaType, String failure, float populationProgress, long size, double valuesSelectivity, String userDescription) {
        this.name = name;
        this.label = label;
        this.properties = properties;
        this.status = status;
        this.type = schemaType;
        this.failure = failure;
        this.populationProgress = populationProgress;
        this.size = size;
        this.valuesSelectivity = valuesSelectivity;
        this.userDescription = userDescription;
    }
}
