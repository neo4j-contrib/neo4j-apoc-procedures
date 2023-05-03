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
package apoc.schema;

import java.util.*;

/**
 * @author ab-larus
 * @since 17.12.18
 */
public class SchemaConfig {
    private static final String LABELS_KEY = "labels";
    private static final String EXCLUDE_LABELS_KEY = "excludeLabels";
    private static final String RELATIONSHIPS_KEY = "relationships";
    private static final String EXCLUDE_RELATIONSHIPS_KEY = "excludeRelationships";
    
    private final Set<String> labels;
    private final Set<String> excludeLabels;
    private final Set<String> relationships;
    private final Set<String> excludeRelationships;

    public Set<String> getLabels() {
        return labels;
    }

    public Set<String> getExcludeLabels() {
        return excludeLabels;
    }

    public Set<String> getRelationships() {
        return relationships;
    }

    public Set<String> getExcludeRelationships() {
        return excludeRelationships;
    }

    public SchemaConfig(Map<String,Object> config) {
        config = config != null ? config : Collections.emptyMap();
        this.labels = new HashSet<>((Collection<String>)config.getOrDefault(LABELS_KEY, Collections.EMPTY_SET));
        this.excludeLabels = new HashSet<>((Collection<String>) config.getOrDefault(EXCLUDE_LABELS_KEY, Collections.EMPTY_SET));
        validateParameters(this.labels, this.excludeLabels, LABELS_KEY, EXCLUDE_LABELS_KEY);
        this.relationships = new HashSet<>((Collection<String>)config.getOrDefault(RELATIONSHIPS_KEY, Collections.EMPTY_SET));
        this.excludeRelationships = new HashSet<>((Collection<String>)config.getOrDefault(EXCLUDE_RELATIONSHIPS_KEY, Collections.EMPTY_SET));
        validateParameters(this.relationships, this.excludeRelationships, RELATIONSHIPS_KEY, EXCLUDE_RELATIONSHIPS_KEY);
    }

    private void validateParameters(Set<String> include, Set<String> exclude, String includeParameterType, String excludeParameterType){
        if(!include.isEmpty() && !exclude.isEmpty())
            throw new IllegalArgumentException( String.format("Parameters %s and %s are both valuated. Please check parameters and valuate only one.", 
                    includeParameterType, 
                    excludeParameterType) );
    }
}
