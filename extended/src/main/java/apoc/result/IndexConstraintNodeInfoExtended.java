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

import org.neo4j.procedure.Description;

import java.util.List;

/**
 * Created by alberto.delazzari on 04/07/17.
 */
public class IndexConstraintNodeInfoExtended {

    @Description("A generated name for the index or constraint.")
    public final String name;

    @Description("The label associated with the constraint or index.")
    public final Object label;

    @Description("The property keys associated with the constraint or index.")
    public final List<String> properties;

    @Description("The status of the constraint or index.")
    public final String status;

    @Description("The type of the index or constraint.")
    public final String type;

    @Description("If a failure has occurred.")
    public final String failure;

    @Description("The percentage of the constraint or index population. ")
    public final double populationProgress;

    @Description("The number of entries in the given constraint or index.")
    public final long size;

    @Description("A ratio between 0.0 and 1.0, representing how many unique values were seen from the sampling.")
    public final double valuesSelectivity;

    @Description("A descriptor of the constraint or index.")
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
    public IndexConstraintNodeInfoExtended(
            String name,
            Object label,
            List<String> properties,
            String status,
            String schemaType,
            String failure,
            float populationProgress,
            long size,
            double valuesSelectivity,
            String userDescription) {
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
