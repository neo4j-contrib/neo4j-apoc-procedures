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

import apoc.SystemPropertyKeys;
import org.neo4j.graphdb.Node;

import java.util.Collections;
import java.util.Map;

import static apoc.uuid.UuidConfig.ADD_TO_SET_LABELS_KEY;
import static apoc.uuid.UuidConfig.UUID_PROPERTY_KEY;

public class UuidInfo {
    public final String label;
    public final boolean installed;
    public final Map<String, Object> properties;

    UuidInfo(String label, boolean installed, Map<String, Object> properties) {
        this.label = label;
        this.installed = installed;
        this.properties = properties;
    }

    UuidInfo(boolean installed) {
        this(null, installed, Collections.emptyMap());
    }

    public UuidInfo(Node node, boolean installed) {
        this.label = (String) node.getProperty(SystemPropertyKeys.label.name());
        boolean addToSetLabel = (boolean) node.getProperty(SystemPropertyKeys.addToSetLabel.name());
        String propertyName = (String) node.getProperty(SystemPropertyKeys.propertyName.name());
        this.properties = Map.of(UUID_PROPERTY_KEY, propertyName,
                ADD_TO_SET_LABELS_KEY, addToSetLabel);
        this.installed = installed;
    }

    public UuidInfo(Node node) {
        this(node, false);
    }
}