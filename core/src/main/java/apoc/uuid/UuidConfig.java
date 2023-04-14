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
package apoc.uuid;

import java.util.Collections;
import java.util.Map;

import static apoc.util.Util.toBoolean;

public class UuidConfig {

    private boolean createConstraint = true;
    private boolean addToExistingNodes;
    private boolean addToSetLabels;
    private String uuidProperty;
    public static final String ADD_TO_SET_LABELS_KEY = "addToSetLabels";
    public static final String UUID_PROPERTY_KEY = "uuidProperty";
    public static final String ADD_TO_EXISTING_NODES_KEY = "addToExistingNodes";

    public static final String DEFAULT_UUID_PROPERTY = "uuid";
    private static final boolean DEFAULT_ADD_TO_EXISTING_NODES = true;
    public static final boolean DEFAULT_ADD_TO_SET_LABELS = false;


    public UuidConfig(Map<String, Object> config) {
        if (config == null) {
            config = Collections.emptyMap();
        }
        this.addToExistingNodes = toBoolean(config.getOrDefault(ADD_TO_EXISTING_NODES_KEY, DEFAULT_ADD_TO_EXISTING_NODES));
        this.addToSetLabels = toBoolean(config.getOrDefault(ADD_TO_SET_LABELS_KEY, DEFAULT_ADD_TO_SET_LABELS));
        this.uuidProperty = config.getOrDefault(UUID_PROPERTY_KEY, DEFAULT_UUID_PROPERTY).toString();

    }

    public UuidConfig() {} // for Jackson deserialization

    public boolean isAddToExistingNodes() {
        return addToExistingNodes;
    }

    public boolean isCreateConstraint() {
        return createConstraint;
    }

    public void setCreateConstraint(boolean createConstraint) {
        this.createConstraint = createConstraint;
    }

    public void setAddToExistingNodes(boolean addToExistingNodes) {
        this.addToExistingNodes = addToExistingNodes;
    }

    public String getUuidProperty() {
        return uuidProperty;
    }

    public void setUuidProperty(String uuidProperty) {
        this.uuidProperty = uuidProperty;
    }

    public boolean isAddToSetLabels() {
        return addToSetLabels;
    }
}
