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
package apoc.meta.tablesforlabels;

import apoc.meta.Meta.ConstraintTracker;
import org.neo4j.graphdb.Entity;

import java.util.*;

/**
 * A profile of a particular ordered label set (or relationship type) has a set of possible properties that can exist, and
 * stats about those properties.
 */
public class PropertyContainerProfile {
    public long observations;
    public boolean isNode;
    Map<String, PropertyTracker> profile;

    public PropertyContainerProfile() {
        observations = 0;
        profile = new HashMap<>(3);
        isNode = false;
    }

    public Set<String> propertyNames() { return profile.keySet(); }
    public PropertyTracker trackerFor(String propName) { return profile.get(propName); }

    public void observe(Entity n, boolean isNode) {
        observations++;

        for (String propName : n.getPropertyKeys()) {
            PropertyTracker tracker;

            if (profile.containsKey(propName)) {
                tracker = profile.get(propName);
            } else {
                tracker = new PropertyTracker(propName);
                profile.put(propName, tracker);
            }

            tracker.addObservation(n.getProperty(propName));

            tracker.mandatory = false;
            this.isNode = isNode;
        }
    }

    public PropertyContainerProfile finished() {
        PropertyTracker tracker;

        for (String propName : this.propertyNames()) {
            if (this.isNode) {

                // Check for node constraints

                for (Map.Entry<String,List<String>> entry : ConstraintTracker.nodeConstraints.entrySet()) {
                    for (String pk : entry.getValue()) {
                        if (this.profile.containsKey(pk)) {
                            tracker = this.profile.get(pk);
                            tracker.mandatory = true;
                        }
                    }
                }
            } else {

                // Check for relationship constraints

                for (Map.Entry<String,List<String>> entry : ConstraintTracker.relConstraints.entrySet()) {
                    for (String pk : entry.getValue()) {
                        if (this.profile.containsKey(pk)) {
                            tracker = this.profile.get(pk);
                            tracker.mandatory = true;
                        }
                    }
                }
            }
        }

        return this;
    }
}
